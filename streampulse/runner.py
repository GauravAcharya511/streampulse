"""StreamRunner -- drives a Pipeline over a source with periodic checkpoints and
crash recovery.

On run(): if the checkpoint store holds a checkpoint, restore each operator's
state and resume the source from the offset AFTER the checkpointed one; otherwise
start at 0. Every `checkpoint_every` events, snapshot all operators + the current
offset and commit atomically. On end of stream, flush and (optionally) checkpoint.

This is the mechanism behind exactly-once recovery (v0.6): because working state
is in-memory and only checkpoints are durable, a crash discards everything since
the last checkpoint, and replay from the checkpointed offset reproduces exactly
the same result -- no loss, no double count.
"""
from typing import List

from .event import Event


class StreamRunner:
    def __init__(self, pipeline, store=None, checkpoint_every: int = 0):
        self.pipeline = pipeline
        self.store = store
        self.checkpoint_every = checkpoint_every
        self.restored_from = None       # offset restored from, or None

    def _ops(self):
        return self.pipeline._operators

    def _cascade_push(self, start_index: int, events) -> List[Event]:
        stream = list(events)
        for op in self._ops()[start_index:]:
            nxt = []
            for e in stream:
                nxt.extend(op.push(e))
            stream = nxt
        return stream

    def _drain_flush(self) -> List[Event]:
        out = []
        for i, op in enumerate(self._ops()):
            flushed = list(op.flush())
            if flushed:
                out.extend(self._cascade_push(i + 1, flushed))
        return out

    def _checkpoint(self, offset: int) -> None:
        snaps = [op.snapshot() for op in self._ops()]
        self.store.save(offset, snaps)

    def run(self, source, final_checkpoint: bool = False) -> List[Event]:
        start = 0
        if self.store is not None:
            ckpt = self.store.load()
            if ckpt is not None:
                for op, snap in zip(self._ops(), ckpt["operators"]):
                    op.restore(snap)
                start = ckpt["offset"] + 1
                self.restored_from = ckpt["offset"]

        out: List[Event] = []
        n = 0
        last_offset = start - 1
        for off, event in source.read_from(start):
            out.extend(self._cascade_push(0, [event]))
            last_offset = off
            n += 1
            if self.store is not None and self.checkpoint_every and n % self.checkpoint_every == 0:
                self._checkpoint(off)

        out.extend(self._drain_flush())
        if self.store is not None and final_checkpoint:
            self._checkpoint(last_offset)
        return out

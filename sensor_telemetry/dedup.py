"""Per-device sequence tracking: idempotency, regression and conflict detection.

Admission rules under a single per-device lock (check and commit are atomic):

* seq already accepted:
    - same fingerprint              -> DUPLICATE (idempotent, counted once)
    - different fingerprint         -> CONFLICT
* timestamp older than the device
  watermark by more than allowed   -> TOO_LATE
* unseen seq below max_seq but with
  a non-retrograde timestamp        -> SEQUENCE_REGRESSION (the device's
                                       counter went backwards; this is not a
                                       late gap-fill, whose timestamp would
                                       precede the watermark)
* unseen seq below max_seq with an
  older but in-tolerance timestamp  -> admitted as a late gap-fill
* otherwise                         -> admitted, advances max_seq/watermark
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from .errors import ErrorCode
from .models import DeviceState, Reading


@dataclass(frozen=True, slots=True)
class Admission:
    """Result of the per-device sequence admission check."""

    admitted: bool
    duplicate: bool = False
    reason: str | None = None
    error_code: ErrorCode | None = None


class SequenceTracker:
    """Holds per-device state; each device gets its own lock, so concurrent
    reports for the same device are serialized without double counting or
    state corruption, while different devices never block each other."""

    def __init__(self, max_lateness_seconds: float = 300.0) -> None:
        self._max_lateness = max_lateness_seconds
        self._devices: dict[str, tuple[threading.Lock, DeviceState]] = {}
        self._registry_lock = threading.Lock()

    def _device(self, device_id: str) -> tuple[threading.Lock, DeviceState]:
        entry = self._devices.get(device_id)
        if entry is not None:
            return entry
        with self._registry_lock:
            entry = self._devices.get(device_id)
            if entry is None:
                entry = (threading.Lock(), DeviceState(device_id=device_id))
                self._devices[device_id] = entry
            return entry

    def check_and_commit(self, reading: Reading) -> Admission:
        """Atomically classify and (when admitted) record the reading."""
        lock, state = self._device(reading.device_id)
        with lock:
            existing = state.accepted.get(reading.seq)
            if existing is not None:
                if existing.fingerprint() == reading.fingerprint():
                    return Admission(
                        admitted=False,
                        duplicate=True,
                        reason=(
                            f"duplicate report for seq={reading.seq} with identical "
                            "content; treated as idempotent success"
                        ),
                    )
                return Admission(
                    admitted=False,
                    reason=(
                        f"conflicting report for seq={reading.seq}: stored "
                        f"fingerprint {existing.fingerprint()} != incoming "
                        f"{reading.fingerprint()}"
                    ),
                    error_code=ErrorCode.CONFLICT,
                )

            # Lateness is measured against the highest timestamp already
            # admitted for the device (the device watermark). A report that is
            # too far behind can never be admitted, whether it fills a gap or
            # extends the sequence.
            behind = state.latest_ts - reading.timestamp
            if state.max_seq >= 0 and behind > self._max_lateness:
                return Admission(
                    admitted=False,
                    reason=(
                        f"too late: timestamp {reading.timestamp} is "
                        f"{behind:.3f}s behind device watermark "
                        f"{state.latest_ts}, exceeding max lateness "
                        f"{self._max_lateness}s"
                    ),
                    error_code=ErrorCode.TOO_LATE,
                )

            # An unseen seq below the contiguous frontier while the timestamp
            # is not retrograde means the device counter itself went backwards.
            # (A genuine late gap-fill would carry an older timestamp and be
            # admitted by the branch below.)
            if state.max_seq >= 0 and reading.seq < state.max_seq and behind <= 0:
                return Admission(
                    admitted=False,
                    reason=(
                        f"sequence regression: unseen seq={reading.seq} below "
                        f"frontier max_seq={state.max_seq} with non-retrograde "
                        f"timestamp {reading.timestamp} (watermark "
                        f"{state.latest_ts})"
                    ),
                    error_code=ErrorCode.SEQUENCE_REGRESSION,
                )

            state.accepted[reading.seq] = reading
            if reading.seq > state.max_seq:
                state.max_seq = reading.seq
            if reading.timestamp > state.latest_ts:
                state.latest_ts = reading.timestamp
            return Admission(admitted=True, reason="admitted")

    def state_of(self, device_id: str) -> DeviceState | None:
        entry = self._devices.get(device_id)
        return None if entry is None else entry[1]

    def latest_seq(self, device_id: str) -> int | None:
        state = self.state_of(device_id)
        return None if state is None else state.max_seq

    def count_accepted(self, device_id: str) -> int:
        state = self.state_of(device_id)
        return 0 if state is None else len(state.accepted)

    def reset(self) -> None:
        with self._registry_lock:
            self._devices.clear()

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class InMemoryJobStore:
    ttl_seconds: int
    _records: Dict[str, dict] = field(default_factory=dict)

    def get(self, job_id: str) -> dict | None:
        return self._records.get(job_id)

    def set(self, job_id: str, record: dict) -> None:
        self._records[job_id] = record

    def delete(self, job_id: str) -> None:
        self._records.pop(job_id, None)

    def all(self) -> Dict[str, dict]:
        return dict(self._records)

    def cleanup(self, now: float) -> list[str]:
        stale = [
            job_id for job_id, record in self._records.items()
            if record.get("created_at", now) < now - self.ttl_seconds
        ]
        for job_id in stale:
            self.delete(job_id)
        return stale

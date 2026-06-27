from dataclasses import dataclass, field


@dataclass
class SynthesisResult:
    success: bool
    stdout: str
    stderr: str
    returncode: int
    reports: dict[str, str] = field(default_factory=dict)  # filename -> contents

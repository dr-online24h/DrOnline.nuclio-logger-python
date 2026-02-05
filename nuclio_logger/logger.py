import json
import sys
from datetime import datetime, timezone


class NuclioLogger:
    """Logger padronizado para funcoes Nuclio. Saida em JSON estruturado."""

    LEVELS = {
        "DEBUG":    0,
        "INFO":     1,
        "WARNING":  2,
        "ERROR":    3,
        "CRITICAL": 4,
    }

    def __init__(self, service: str = "unnamed", min_level: str = "INFO"):
        """
        Args:
            service:   nome da funcao/servico (aparece no campo 'service' do log).
            min_level: nivel minimo de log (DEBUG | INFO | WARNING | ERROR | CRITICAL).
        """
        self.service   = service
        self.min_level = self.LEVELS.get(min_level.upper(), 1)

    # ------------------------------------------------------------------
    # interno
    # ------------------------------------------------------------------
    def _log(self, level: str, message: str, context: dict | None = None) -> None:
        if self.LEVELS.get(level, 0) < self.min_level:
            return

        entry: dict = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") +
                         f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z",
            "level":     level,
            "service":   self.service,
            "message":   message,
        }

        if context is not None:
            entry["context"] = context

        # flush=True garante saida imediata no stdout do container
        print(json.dumps(entry, ensure_ascii=False), flush=True)

    # ------------------------------------------------------------------
    # publico
    # ------------------------------------------------------------------
    def debug(self, message: str, context: dict | None = None) -> None:
        self._log("DEBUG", message, context)

    def info(self, message: str, context: dict | None = None) -> None:
        self._log("INFO", message, context)

    def warning(self, message: str, context: dict | None = None) -> None:
        self._log("WARNING", message, context)

    def error(self, message: str, context: dict | None = None) -> None:
        self._log("ERROR", message, context)

    def critical(self, message: str, context: dict | None = None) -> None:
        self._log("CRITICAL", message, context)

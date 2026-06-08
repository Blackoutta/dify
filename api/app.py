import os
import signal
import sys
from pathlib import Path


def is_db_command():
    if len(sys.argv) > 1 and sys.argv[0].endswith("flask") and sys.argv[1] == "db":
        return True
    return False


# create app
if is_db_command():
    from app_factory import create_migrations_app

    app = create_migrations_app()
else:
    # It seems that JetBrains Python debugger does not work well with gevent,
    # so we need to disable gevent in debug mode.
    # If you are using debugpy and set GEVENT_SUPPORT=True, you can debug with gevent.
    if (flask_debug := os.environ.get("FLASK_DEBUG", "0")) and flask_debug.lower() in {"false", "0", "no"}:
        from gevent import monkey

        # gevent
        monkey.patch_all()

        from grpc.experimental import gevent as grpc_gevent  # type: ignore

        # grpc gevent
        grpc_gevent.init_gevent()

        import psycogreen.gevent  # type: ignore

        psycogreen.gevent.patch_psycopg()

        if os.environ.get("DIFY_ENABLE_GEVENT_RUN_INFO", "0").lower() in {"1", "true", "yes"}:
            from gevent.util import format_run_info

            def dump_gevent_run_info(signum, frame):  # type: ignore[no-untyped-def]
                del signum, frame
                output_path = Path(f"/tmp/dify-gevent-run-info-{os.getpid()}.log")
                run_info = format_run_info()
                content = run_info if isinstance(run_info, str) else "\n".join(run_info)
                output_path.write_text(content)

            signal.signal(signal.SIGUSR2, dump_gevent_run_info)

    from app_factory import create_app

    app = create_app()
    celery = app.extensions["celery"]

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)

#!/usr/bin/env python3
import asyncio
import getpass
import sys
from dataclasses import replace

from webqq_tui_app.client import AuthenticationError, WebQQClient, WebQQClientError
from webqq_tui_app.config import TuiConfig


async def async_main(argv=None) -> int:
    try:
        config = TuiConfig.from_args(argv)
    except ValueError as exc:
        print("webqq-tui: {}".format(exc), file=sys.stderr)
        return 2

    client = WebQQClient(config)
    await client.start()
    try:
        try:
            await client.login()
        except AuthenticationError:
            if config.token is not None:
                raise
            token = getpass.getpass("WebQQ token: ")
            await client.login(token)
            config = replace(config, token=token)
            client.config = config

        from webqq_tui_app.app import WebQQTui

        app = WebQQTui(client)
        await app.run_async()
        return 0
    except (AuthenticationError, WebQQClientError) as exc:
        print("webqq-tui: {}".format(exc), file=sys.stderr)
        return 1
    finally:
        await client.close()


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())

# WebQQ

A lightweight web-based QQ client that connects to a local [NapCat](https://github.com/NapNeko/NapCatQQ) server, records recent messages, and provides a browser interface for reading and sending messages.

## Features

- Real-time message sync via WebSocket
- Last 1000 messages per chat (group / private) persisted to disk
- Browser-based UI with dark theme
- Token-based web login
- Automatic NapCat reconnection
- Chat list with friend/group name resolution

## Requirements

- Python 3.8+
- A running NapCat instance on localhost

## Quick Start

1. Install NapCat and add a **WebSocket Server** (not client) listener. Pick any port (`napcat_port`) and copy the generated token.
2. `pip install -r requirements.txt`
3. `cp config.json.example config.json`
4. Edit `config.json`:
   - Set `napcat_token` to the token from step 1
   - Set `ws_url` to `ws://localhost:{napcat_port}/?message_post_format=array`
   - Set `web_token` to your desired browser login password
5. `python3 webqq.py`

Then open `http://localhost:8080` (or your configured `web_port`) in a browser.

## Configuration

Configuration is stored in `config.json` (gitignored). A template is provided in `config.json.example`.

| Key              | Default                                                | Description                                       |
| ---------------- | ------------------------------------------------------ | ------------------------------------------------- |
| `ws_url`         | `ws://localhost:{port}/?message_post_format=array`     | NapCat WebSocket URL                              |
| `napcat_token`   | *(empty)*                                              | Token for authenticating with the NapCat WebSocket server                    |
| `web_port`       | `8080`                                                 | Port for the web UI server                        |
| `web_token`      | *(empty)*                                              | Password for browser login. Empty = no auth required |
| `flush_interval` | `15`                                                   | Seconds between message-to-disk flushes           |

- `config.json` is auto-created from defaults on first run if missing.
- `config.json.example` tracks the schema and should be committed to git.

## Data Storage

Messages are stored under `data/` as one JSON file per chat (`data/group_12345.json`, `data/private_67890.json`). Each file holds up to 1000 messages. The `data/` directory is gitignored.

## API

| Method | Path                               | Description              |
| ------ | ---------------------------------- | ------------------------ |
| POST   | `/api/login`                       | Authenticate with token  |
| GET    | `/api/chats`                       | List chats               |
| GET    | `/api/messages?chat_id=X&limit=50` | Get messages (paginated) |
| POST   | `/api/send`                        | Send a message           |
| GET    | `/api/status`                      | Connection status        |
| WS     | `/ws`                              | Real-time message feed   |

## License

GPLv3 — see [LICENSE](LICENSE).

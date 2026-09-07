# DON'T ACT HUMAN relay / Render test deployment

1. Put the contents of this directory at the root of a new GitHub or GitLab repository.
2. In Render, choose **New > Blueprint**, connect that repository, and deploy `render.yaml`.
3. Wait until the deploy and `/healthz` health check succeed.
4. If Render shows `https://dont-act-human-relay-xxxx.onrender.com`, enter
   `wss://dont-act-human-relay-xxxx.onrender.com` in the game's first online setup screen.
5. Every tester uses the same URL. The host creates a room and shares the 12-character room code.

A Render account and hosted Git repository are required. The free plan is suitable only for a short test;
its WebSocket connection limit can end a session after five minutes.

Server integration test:

```bash
python -m pip install -r requirements.txt
python -m unittest -v test_relay.py
```

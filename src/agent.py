"""
agent.py

Runs a Snowflake Cortex Agent grounded on the HP series Cortex Search Service.
Demonstrates that the agent counts spells from the actual document (including
injected fake spells) rather than relying on training data.

Usage:
    export PRIVATE_KEY_PASSPHRASE="your passphrase"
    python agent.py
    python agent.py --question "List every spell mentioned in the series"
"""

import argparse
import base64
import datetime
import hashlib
import json
import os
import sys
from pathlib import Path

import jwt
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key

SNOWFLAKE_ACCOUNT = "WJTXXEX-HOC83051"
SNOWFLAKE_USER = "HP_DEMO_SVC"
SNOWFLAKE_ROLE = "SYSADMIN"
SNOWFLAKE_WAREHOUSE = "HP_DEMO_WH"
PRIVATE_KEY_PATH = Path(__file__).parent.parent / "keys" / "rsa_key.p8"

AGENT_BASE_URL = (
    f"https://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"
    "/api/v2/databases/HP_DEMO/schemas/SPELLS/agents"
)

DEFAULT_QUESTION = (
    "Search the Harry Potter series thoroughly and list every spell or incantation "
    "mentioned in the books. Include the exact spell name, which book it appeared in, "
    "and who cast it. Then give me a total count of unique spells found."
)


def get_jwt_token() -> str:
    """Build a Snowflake keypair JWT for REST API auth."""
    passphrase = os.environ.get("PRIVATE_KEY_PASSPHRASE")
    if not passphrase:
        raise EnvironmentError("Set PRIVATE_KEY_PASSPHRASE environment variable first.")

    private_key = load_pem_private_key(
        PRIVATE_KEY_PATH.read_bytes(),
        password=passphrase.encode(),
        backend=default_backend(),
    )

    # Compute the public key fingerprint (SHA-256 of DER-encoded public key)
    public_key_der = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    fingerprint = base64.b64encode(hashlib.sha256(public_key_der).digest()).decode()

    # Snowflake JWT claims
    account = SNOWFLAKE_ACCOUNT.upper().split(".")[0]  # strip region suffix if present
    qualified_user = f"{account}.{SNOWFLAKE_USER.upper()}"
    now = datetime.datetime.utcnow()

    payload = {
        "iss": f"{qualified_user}.SHA256:{fingerprint}",
        "sub": qualified_user,
        "iat": now,
        "exp": now + datetime.timedelta(minutes=59),
    }

    return jwt.encode(payload, private_key, algorithm="RS256")


def run_agent(question: str, agent_name: str = "HP_SPELL_AGENT", verbose: bool = False) -> None:
    url = f"{AGENT_BASE_URL}/{agent_name}:run"
    print(f"\nAgent:    {agent_name}")
    print(f"Question: {question}\n")
    print("-" * 70)

    token = get_jwt_token()

    payload = {
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": question}],
            }
        ],
        "stream": True,
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
    }

    response = requests.post(
        url,
        headers=headers,
        json=payload,
        stream=True,
        timeout=120,
    )

    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")
        sys.exit(1)

    # Stream and print the response — SSE format: alternating "event:" and "data:" lines
    current_event_type = None
    for line in response.iter_lines():
        if not line:
            current_event_type = None
            continue

        raw = line.decode("utf-8")

        if raw.startswith("event: "):
            current_event_type = raw[7:].strip()
            continue

        if raw.startswith("data: "):
            raw = raw[6:]
        else:
            continue

        if raw in ("[DONE]", ""):
            continue

        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            continue

        if verbose:
            print(f"[{current_event_type}] {json.dumps(event, indent=2)}")
            continue

        if current_event_type == "response.text.delta":
            print(event.get("text", ""), end="", flush=True)

        elif current_event_type == "response.tool_use":
            query = event.get("input", {}).get("query", "")
            if query:
                print(f"\n[searching: \"{query}\"]\n", flush=True)

        elif current_event_type == "response.status":
            status = event.get("status", "")
            message = event.get("message", "")
            if status not in ("executing_tools",):
                print(f"\n[{message}]", flush=True)

    print("\n" + "-" * 70)


def main():
    parser = argparse.ArgumentParser(description="HP Cortex Agent — spell grounding experiment")
    parser.add_argument("--question", default=DEFAULT_QUESTION, help="Question to ask the agent")
    parser.add_argument("--agent", default="HP_SPELL_AGENT", help="Agent name (default: HP_SPELL_AGENT)")
    parser.add_argument("--verbose", action="store_true", help="Print raw SSE events")
    args = parser.parse_args()

    run_agent(args.question, agent_name=args.agent, verbose=args.verbose)


if __name__ == "__main__":
    main()

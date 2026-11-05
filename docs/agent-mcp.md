# juicemount-mcp: letting an AI agent find files

`juicemount-mcp` is a small MCP server that runs on a Mac. An agent such as
Claude Desktop or Claude Code starts it and talks to it over stdin and stdout.
Through it the agent can find files on the JuiceMount volume and curate
collections, as you and only within your project grants. It never reads or
streams file contents: every result carries `mount_path`, the file's path
under this Mac's mount point, and the agent opens that with its own file
tools.

It needs a Manager with `JM_CONTINUITY=on` (see "Agent discovery" in
`server/README.md`) and a Mac that the Manager has approved and assigned to
you on the People page.

## Set it up in the app

JuiceMount bundles the server at
`/Applications/JuiceMount.app/Contents/MacOS/juicemount-mcp`. Open
JuiceMount's **Settings → Connection → Agents**. The section shows once this
Mac is approved (Server access, just above it) and the Manager has agent access
on; otherwise it says why not.

- **New token…** asks for a name (say, the agent that will use it) and a
  lifetime of up to 90 days. The app mints the token as this Mac and saves it
  in the login Keychain for `juicemount-mcp`. It is never shown or copied.
  Creating another replaces the one agents on this Mac use; the old one keeps
  working on the Manager until you revoke it.
- The list shows your tokens (from all your Macs) with when each was created
  and expires; **On this Mac** marks the one stored here. **Revoke** stops a
  token at once, and deletes it here when it is the stored one.
- **Copy Claude Desktop config** and **Copy Claude Code command** put the setup
  for the bundled server and your mount point on the clipboard (below).

The app keeps the token in a Keychain item whose access list trusts the app
and the bundled `juicemount-mcp`, so `serve` reads it without asking, and the
app's own enrollment record is never opened to another program. This needs
the release build (Developer ID): for an ad-hoc build macOS refuses even a
listed program, and `serve` cannot read the token.

## Add it to Claude Desktop

**Copy Claude Desktop config** gives, for the default mount point:

```json
{
  "mcpServers" : {
    "juicemount" : {
      "args" : [
        "serve",
        "--mount",
        "/Volumes/zpool"
      ],
      "command" : "/Applications/JuiceMount.app/Contents/MacOS/juicemount-mcp"
    }
  }
}
```

Merge it into `~/Library/Application Support/Claude/claude_desktop_config.json`
and restart Claude Desktop. `--mount` is your volume's mount point (Settings →
Connection → Volume; the default is `/Volumes/zpool`, also read from
`$JUICEMOUNT_MOUNT`).

## Add it to Claude Code

**Copy Claude Code command** gives:

```sh
claude mcp add juicemount -- /Applications/JuiceMount.app/Contents/MacOS/juicemount-mcp serve --mount /Volumes/zpool
```

Add `--scope user` after `add` to use it in every project.

## Without the app: the `token` commands

The `token` commands are for a machine without JuiceMount's Settings at hand
(headless use, or another machine). On a Mac, use the bundled binary. To build
it yourself:

```sh
go build -o juicemount-mcp ./cmd/juicemount-mcp
```

Build on the Mac with cgo on (the default there): the token is kept in the
login Keychain, and a darwin build without cgo refuses to store one. Linux
builds keep the token in `~/.config/juicemount/credential-agent-token.json`
(mode 0600).

```sh
juicemount-mcp token create --name "Claude Desktop"          # 90 days
juicemount-mcp token create --name "Claude Code" --days 30
```

This asks the Manager with this Mac's own enrollment credentials, which the
JuiceMount app keeps in the Keychain; macOS asks whether `juicemount-mcp` may
read them. The new token goes into the Keychain and is never printed. Minting
again replaces the stored token (the old one keeps working on the Manager
until you revoke it).

```sh
juicemount-mcp token list            # your tokens; * marks the one stored here
juicemount-mcp token revoke <id>     # revoke on the Manager, delete it here
```

For a machine that cannot mint (no enrolled JuiceMount), print a token on
your Mac and import it there. `--print` writes the token to stdout only and
does not store it:

```sh
juicemount-mcp token create --name "build box" --print | \
  ssh build-box juicemount-mcp token import --manager https://nas.example:30190
```

`token revoke <id> --local-only` deletes an imported token without asking the
Manager. If the Manager address is plain `http://` to another machine, `token
create`, `token import` and `serve` warn on stderr (Settings shows the same
warning): the token then crosses the network unencrypted, as the Mac's
enrollment does. Use an `https://` address where you can.

**Who can mint.** `token create` reads this Mac's enrollment from the
Keychain. macOS asks before `juicemount-mcp` may read it; once you allow it,
anything that can run commands as you (an agent with a shell tool included)
can mint tokens too, for as long as the Keychain lets it. Click **Allow**, not
**Always Allow**, so each mint asks you again; better, mint in Settings, which
never opens the enrollment to another program. After you update or rebuild
`juicemount-mcp`, the `token` commands may ask once more before they can use
the stored token (if this build may not update it at all, they replace it;
clicking Deny never deletes it); `serve` never asks (it runs under an agent,
with no one to answer) and tells the agent to create a token again, or to allow
access in Keychain Access. A token the CLI stored trusts only that build of the
CLI; creating one in Settings replaces it with one the bundled server reads.
The Manager stops a token at once when it expires or is revoked, when you are
removed, or when the Mac that minted it is revoked or reassigned.

## Tools

| Tool | What it does |
|---|---|
| `find_files` | Files by name fragment, folders, collection, kind, extension, `modified_after`, `seen_after`; paged with `cursor`/`next`, and `budget_exhausted` when the Manager cut a page short |
| `get_file` | One file by asset ID |
| `list_collections` | Collections, paged |
| `get_collection_members` | A collection's members with their `mount_path`, paged |
| `create_collection`, `rename_collection` | Create (optionally with members) and rename; no delete |
| `add_to_collection`, `remove_from_collection` | Members by asset ID |
| `list_saved_queries`, `get_saved_query_results` | Saved queries (read only) and their files |

Every change carries an `operation_id`, so a retried call is applied once.
Agents should send their own (the `create_collection` description says so);
when a change's outcome is unknown (a lost answer, a timeout, a cancelled
call) the error names the `operation_id` to retry with. A tool call ends
within 55 seconds, inside the 60 seconds clients commonly wait. Errors come
back to the agent as the tool's result: a revoked or expired token says to run
`juicemount-mcp token create` again (or create one in Settings).

The server speaks MCP 2025-06-18 (2024-11-05 too; JSON-RPC batches are
refused). Tool calls run one at a time: a second one while one runs is told to
wait. `ping` and cancellation are handled while a call runs; a cancelled call
stops and gets no answer, and a cancelled change's `operation_id` is written
to the server's log (stderr).

## What it does not do

- It reaches the Manager over the Mac's ordinary network with normal TLS
  verification (as enrollment does). When the Manager is reachable only
  through JuiceMount Link, minting and serving need a route to it.
- It never opens, stats or follows anything under the mount, and skips (and
  logs) any result whose path would leave the mount point.
- Logs go to stderr; stdout carries protocol messages only.

// /relay: a real command. The relay program answers it; nothing goes through the model.
//   /relay                      the rooms this session listens to, and who is connected
//   /relay help                 one line per command
//   /relay <words…>             the same words as `relay <words…>` in a shell

type Ran = { stdout: string; stderr: string; exitCode: number }
type Engine = {
  command: { register(c: { name: string; description: string }): Promise<unknown> }
  process: { run(argv: string[], o: { timeoutMs: number }): Promise<Ran> }
}
type Next = (e: unknown) => unknown
type On = (event: string, ...rest: unknown[]) => void

export const NOT_LINKED = 'the relay program is not linked yet: start one session with the plugin enabled'

// The plugin's server links ~/.local/bin/relay to itself at every start; until one session has started, the
// link does not exist and the command says so instead of failing with a shell error.
const SCRIPT =
  'p="$HOME/.local/bin/relay"; [ -e "$p" ] || { printf "%s\\n" "$1"; exit 0; }; shift; exec "$p" cmd "$@"'

export const register = (on: On) => {
  on('session.start', async ($: Engine, e: unknown, next: Next) => {
    try {
      await $.command.register({
        name: 'relay',
        description: 'Messages between machines: status | list | join | leave | send | rename | create | invite | accept | remove | leave-room | help',
      })
    } catch {
      // the name is already taken in this session
    }
    return next(e)
  })

  on('command.run', { command: 'relay' }, ($: Engine, e: { args: string }) => answer($, e.args))
}

export async function answer($: Engine, typed: string): Promise<{ text: string }> {
  const args = typed.trim().split(/\s+/).filter(Boolean)
  try {
    const ran = await $.process.run(['sh', '-c', SCRIPT, 'relay', NOT_LINKED, ...args], { timeoutMs: 20000 })
    const said = ran.stdout.trim() || ran.stderr.trim() || `relay ended with code ${ran.exitCode}`
    // The row starts with the command's name: the answer begins on its own line.
    return { text: '\n' + said }
  } catch (err) {
    return { text: `\nrelay could not run: ${err instanceof Error ? err.message : String(err)}` }
  }
}

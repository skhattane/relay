// /relay: a real command. The relay program answers it; nothing goes through the model.
//   /relay                      the rooms this session listens to, and who is connected
//   /relay <words…>             the same words as `relay <words…>` in a shell (relay cmd refuses the few
//                               that only make sense in a shell)

type Ran = { stdout: string; stderr: string; exitCode: number }
type Engine = {
  command: { register(c: { name: string; description: string }): Promise<unknown> }
  process: { run(argv: string[], o: { timeoutMs: number }): Promise<Ran> }
}
type Next = (e: unknown) => unknown
type On = (event: string, ...rest: unknown[]) => void

export const register = (on: On) => {
  on('session.start', async ($: Engine, e: unknown, next: Next) => {
    try {
      await $.command.register({
        name: 'relay',
        description: 'Messages between machines: status | list | join <room> | leave <room> | send <[room:]name|all> <text> | rename | invite | …',
      })
    } catch {
      // the name is already taken in this session
    }
    return next(e)
  })

  on('command.run', { command: 'relay' }, ($: Engine, e: { args: string }) => answer($, e.args))
}

async function answer($: Engine, typed: string): Promise<{ text: string }> {
  const args = typed.trim().split(/\s+/).filter(Boolean)
  try {
    // The program lives in the person's own bin folder on every machine; the shell only resolves $HOME.
    const ran = await $.process.run(['sh', '-c', 'exec "$HOME/.local/bin/relay" cmd "$@"', 'relay', ...args], { timeoutMs: 20000 })
    const said = ran.stdout.trim() || ran.stderr.trim() || `relay ended with code ${ran.exitCode}`
    // The row starts with the command's name: the answer begins on its own line.
    return { text: '\n' + said }
  } catch (err) {
    return { text: `\nrelay could not run: ${err instanceof Error ? err.message : String(err)}` }
  }
}

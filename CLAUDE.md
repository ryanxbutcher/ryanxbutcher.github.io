# ryanxbutcher.github.io

Ryan's **public portfolio**, served by GitHub Pages from `main`. Currently one
showcase: the EMS Data Warehouse assessment — `index.html` plus the
`sql/`, `etl/` and `config/` material it presents.

## Posture

- **Public means public.** Everything here is world-readable and indexed.
  Nothing from prime-cut, no client or employer material, no PHI, no
  internal names — this repo is separate from the rest of the estate on
  purpose, and that separation is the whole point of it.
- **It is a portfolio, not a project.** Content here is presentation of work
  done elsewhere. Build nothing here that wants a home of its own.

## Leaving `main` good

Ryan is the sole contributor. There is no reviewer and no PR queue.
**`main` is where work lives; a branch is a temporary container, never a
destination** — and here `main` is literally the published site, so a branch
is a change nobody can see.

- Commit and push as each piece of work lands, not once at the finish. A
  session can be closed mid-sentence, and unpushed work dies with it.
- **If the harness put you on a branch, you own the merge.** Web, iOS,
  Remote Control and cloud sessions open on `claude/<slug>`. Finishing
  means: merge `origin/main` in, push the branch, merge the branch into
  `main`, push `main`, then delete the branch on both ends
  (`git push origin --delete <branch>`). A merged branch left on GitHub is
  litter the next session cannot tell from unfinished work.
- **Never hand Ryan a merge.** A close-out that says "ready to merge" is an
  unfinished session.
- **Prove it; do not claim it.** `git status --porcelain` empty and
  `git log --oneline origin/main..HEAD` empty before you say it is done.
  Report what they printed. A push here changes what the public sees, so
  check the page rendered before calling it finished.

## Where this sits

pi-brain's roster (`agents/the-overseer/config/repositories.json`) lists this
repo as the public face. The estate map lives in pi-brain's `CLAUDE.md`.

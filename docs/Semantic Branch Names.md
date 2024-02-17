# Semantic Branch Names

**Format:** `<type>/<task>`

## 🏷️ Types

- `feature`: (new feature for the user, not a new feature for build script)
- `bug`: (bug fix for the user, not a fix to a build script)
- `chore`: (updating grunt tasks etc; no production code change)
- `refactor`: (refactoring production code, eg. renaming a variable)
- `docs`: (changes to the documentation)
- `style`: (formatting, missing semi colons, etc; no production code change)
- `perf`: (production changes related to backward-compatible performance improvements)
- `test`: (adding missing tests, refactoring tests; no production code change)
- `build`: (updating grunt tasks etc; no production code change)
- `ci`: (changes to identify development changes related to the continuous integration and deployment system - involving scripts, configurations or tools)

## 💡 Example

```
feature/exampleTaskName
```

<details>
    <summary>References</summary>
    <ul>
        <li>https://www.conventionalcommits.org/</li>
        <li>https://seesparkbox.com/foundry/semantic_commit_messages</li>
        <li>http://karma-runner.github.io/1.0/dev/git-commit-msg.html</li>
        <li>https://nitayneeman.com/posts/understanding-semantic-commit-messages-using-git-and-angular/</li>
    </ul>
</details>

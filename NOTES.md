# Solution Explanation: HyperGuest Senior BE Developer Task

## Approach & Data Structures

- **No AI Used:** I confirm that I did not use AI tools while working on the main implementation. My initial solution can be seen in this commit: [129dc5e](https://github.com/harut-g/hg-senior-be-developer-task/commit/129dc5e994b3a41c18cc98531d59909e2c2e8183#diff-dd224a3ce931bc25964ef1c60a075f810764a371f52835c3fb2d5b6efe17217a), along with subsequent improvements.

- **Optimized Lookups:**
  - I replaced array-based queues with `Map` structures to achieve O(1) access and cleanup.
  - Two main maps are used:
    - `messages`: Stores messages by ID for fast lookup and deletion.
    - `keysMeta`: Tracks the processing state of each key and which worker is handling it.

## Queue Logic

- **Enqueue:**
  - Messages are added to the `messages` map using their unique IDs.

- **Dequeue:**
  - A worker can dequeue a message if:
    - The key is not currently being processed, or
    - The same worker is already processing that key.
  - This ensures that messages for the same key are handled by only one worker at a time.
  - The worker ID is recorded in `keysMeta` to enforce key-level exclusivity.

- **Confirm:**
  - Once a message is processed, confirm ensures the correct worker is confirming it before removing it from messages.



## Practical Improvements

- **Key Cleanup:**
  - `keysMeta` entries are never cleaned up even when no messages remain for a key.  

- **Worker Message Tracking:**
  - Currently, there’s no reverse lookup of which worker is handling which message(s).  

## Summary

- The solution works reliably for the current scope — multiple workers in a single process.
- It enforces per-key exclusivity and uses efficient data structures.
- A few small changes like key cleanup and optional tracking enhancements can make the implementation cleaner and more maintainable.

---
applyTo: '**'
---
1. **Follow the Request Precisely**
   Execute exactly what is requested — no assumptions, no additions, no creative deviations beyond the user's explicit instructions.

2. **Code Quality and Structure**

   * Keep code **accurate, minimal, and cleanly formatted**.
   * Avoid monolithic structures; prefer modular and maintainable patterns.
   * Ensure readability and clarity in both code and comments.

3. **Documentation Discipline**
   Do **not** create any `summary` or `changelog.md` files unless explicitly required.

4. **Next-Step Guidance**
   After each response, always **propose clear next steps** or logical follow-up actions the user might take.

5. **Execution Protocol**

   * **List** what you intend to do before taking any action.
   * Only **execute** once the user explicitly confirms with “confirm”, “proceed”, or similar approval.
   * Perform **only** the confirmed actions.

6. **Code Standards**
   All generated code must adhere to:

   * **DRY (Don’t Repeat Yourself)**
   * **SOLID Principles**
   * **ACID Properties** (for database-related logic)
   * Professional, production-grade coding conventions.
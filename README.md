# cpark
A light-weighted, distributed computing framework for C++ that offers a fast and general-purpose large data processing solution.

## Code Style Agreements
### Naming Style
- `non_member_local_variable`: snake case without tailing underscore.
- `member_variable_`: snake case with a tailing underscore.
- `g_static_non_member_variable`: snake case with a heading `g_`.
- `g_static_member_variable_`: snake case with a heading `g_` and a tailing underscore.
- `memberFunction()`: lower camel case.
- `nonMemberFunction()`: lower camel case.
- `ClassName`: upper camel case.
- `TemplateName<>`: upper camel case.
- `concepts::ConceptName<>`: upper camel case. All concepts defined by this project should be put in a namespace `concepts`.
- `MACRO_NAME()`: snake case with all characters upper.
### Formatting
Format the code with the `.clang-format` file. Currently, it uses google style.
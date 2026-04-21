class PromptRegistryError(Exception):
    pass


class PromptRegistryValidationError(PromptRegistryError):
    pass


class PromptRegistryNotFoundError(PromptRegistryError):
    pass


class PromptRegistryConflictError(PromptRegistryError):
    pass

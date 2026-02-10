class TaskRuntime:

    def __init__(
        self,
        source=None,
        destination=None,
        is_blocking: bool = False,
        has_next_step: bool = False,
        post_actions=None
    ):
        self.source = source
        self.destination = destination
        self.is_blocking = is_blocking
        self.has_next_step = has_next_step
        self.post_actions = post_actions or []

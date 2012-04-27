class Error(Exception): 
    pass


class EmptyTaskPollResult(Error):
    pass


class LastPage(Error):
    pass

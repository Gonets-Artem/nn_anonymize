class LengthError(Exception):
    def __init__(self, message):
        super().__init__(message)
        

class SaveDetectionError(Exception):
    def __init__(self, message):
        super().__init__(message)


class SaveFindedError(Exception):
    def __init__(self, message):
        super().__init__(message)


class SaveOriginalError(Exception):
    def __init__(self, message):
        super().__init__(message)


class SaveParsedError(Exception):
    def __init__(self, message):
        super().__init__(message)

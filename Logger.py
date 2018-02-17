class Logger:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    @staticmethod
    def header(message):
        print(Logger.HEADER + message + Logger.ENDC)

    def okblue(message):
        print(Logger.OKBLUE + message + Logger.ENDC)

    def okgreen(message):
        print(Logger.OKGREEN + message + Logger.ENDC)

    def warning(message):
        print(Logger.WARNING + message + Logger.ENDC)

    def fail(message):
        print(Logger.FAIL + message + Logger.ENDC)

    def bold(message):
        print(Logger.BOLD + message + Logger.ENDC)

    def header(message):
        print(Logger.UNDERLINE + message + Logger.ENDC)

    def header(message):
        print(Logger.HEADER + message + Logger.ENDC)
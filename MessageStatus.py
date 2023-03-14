import enum


class MessageStatus(enum.Enum):
    IN_PROGRESS = 'IN_PROGRESS'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    MESSAGE_SENT = 'MESSAGE_SENT'
    PAUSED_NO_CREDIT = 'PAUSED_NO_CREDIT'
    UNPAID_INVOICES = 'UNPAID_INVOICES'
    ACTIVE = 'ACTIVE'
    DUPLICATE_MESSAGE = 'DUPLICATE_MESSAGE'
    PAUSED = 'PAUSED'
    INSUFFICIENT_CREDITS = 'INSUFFICIENT_CREDITS'
    STATUS_PENDING = 'STATUS_PENDING'
    SUCCESS = 'SUCCESS'
    USER_OPTED_OUT = 'USER_OPTED_OUT'

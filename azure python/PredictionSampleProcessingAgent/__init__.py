import logging

import azure.functions as func


def main(msg: func.QueueMessage, predSamp: func.Out[func.QueueMessage]) -> None:
    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))

    predSamp.set("*****THIS HAS TRIGGERD AFTER DATA CLEANING****")

import logging

import azure.functions as func


def main(predSamp: func.QueueMessage, allProd: func.Out[func.QueueMessage]) -> None:
    logging.info('Python queue trigger function processed a queue item: %s',
                predSamp.get_body().decode('utf-8'))
    allProd.set("****THIS HAS TRIGGERD AFTER PREDICTION SAMPLE PROCESSING****")
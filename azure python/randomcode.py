# "queueName": "js-queue-items",
{
  "scriptFile": "__init__.py",
  "bindings": [
    {
      "name": "mytimer",
      "type": "timerTrigger",
      "direction": "in",
      "schedule": "*/20 * * * * *"
    },
    {
      "name": "msg",
      "type": "queue",
      "direction": "out",
      "queueName": "myinputqueue",
      "connection": "sftpagentgroupa4ac_STORAGE"
    }
  ]
}

"connection": "sftpagentgroupa4ac_STORAGE",





    for index,row in data.iterrows():
        row["data"] = row['data'] + 1
        logging.info(row)
    
    logging.info(data)
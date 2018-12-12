- standalone start  

        consul agent -data-dir=./data -config-dir=./config -bind=127.0.0.1 -server -bootstrap-expect=1 -ui
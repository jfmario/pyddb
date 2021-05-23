
import os

from pydbb import app

if __name__ == '__main__':
    app.run(host=os.getenv('APP_HOST', 'localhost'),
        port=int(os.getenv('APP_PORT', '8080')))
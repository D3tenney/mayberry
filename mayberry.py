from app import app, api, celery

''' ADD ROUTES '''
from app.resources.health_api import Health
from app.resources.process_api import Process

api.add_resource(Health, '/health')
api.add_resource(Process, '/process')

''' RUN '''
if __name__ == '__main__':
    app.run(host="localhost", port=app.config["PORT"], debug=True)

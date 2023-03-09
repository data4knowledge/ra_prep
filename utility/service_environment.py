from dotenv import load_dotenv
import os

class ServiceEnvironment():
  
  def __init__(self):
    self.load()

  def environment(self):
    if 'PYTHON_ENVIRONMENT' in os.environ:
      return os.environ['PYTHON_ENVIRONMENT']
    else:
      return "development"

  def production(self):
    print("E", self.environment())
    return self.environment() == "production"

  def get(self, name):
    if name in os.environ:
      return os.environ[name]
    else:
      print("ENV GET: Missing")
      return ""

  def load(self):
    load_dotenv(".%s_env" % self.environment())

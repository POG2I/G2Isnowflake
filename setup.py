from distutils.core import setup
import py2exe

setup(console=['g2iSnowflake.py'],
               options = { 'py2exe' : {
                                        'includes' : [ 'snowflake.connector.snow_logging','html.parser','packaging','packaging.version','packaging.specifiers','packaging.requirements' ]
                                       }
                    }
               )


###
## Calling the bulkloader upload service providing our configuration.
## Here you need to replace the url with yours application and provide authentication, via OAuth2, for instance.
###
appcfg.py upload_data --kind=SampleEntity --config_file=entities_config.yaml --url https://your-application.appspot.com/remote_api --filename=db_settings.py

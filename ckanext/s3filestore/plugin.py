# encoding: utf-8
import ckan.plugins as plugins
import ckantoolkit as toolkit
import boto3
import re

import ckanext.s3filestore.uploader
from ckanext.s3filestore.views import resource, uploads
from ckanext.s3filestore.click_commands import upload_resources, upload_assets
from ckantoolkit import config
from datetime import datetime, timedelta


REGION_NAME = config.get('ckanext.s3filestore.region_name')
AWS_ACCESS_KEY_ID = config.get('ckanext.s3filestore.aws_access_key_id')
AWS_SECRET_ACCESS_KEY = config.get('ckanext.s3filestore.aws_secret_access_key')
BUCKET_NAME = config.get('ckanext.s3filestore.aws_bucket_name')
aws_bucket_url = toolkit.config.get('ckanext.s3filestore.aws_bucket_url')


def sign_url(key_path):
    s3_client = boto3.client(
        's3',
        region_name=REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    url = s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': BUCKET_NAME, 'Key': key_path, },
        ExpiresIn=604800,
        )
    return url


def sigiture_expired(time):
    dt = datetime.strptime(time, "%Y%m%dT%H%M%SZ")
    expiry_time = dt + timedelta(days=7)
    now = datetime.now()
    if now > expiry_time:
        return True
    else:
        return False


class S3FileStorePlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IConfigurable)
    plugins.implements(plugins.IUploader)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.IClick)
    plugins.implements(plugins.IResourceController, inherit=True)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        # We need to register the following templates dir in order
        # to fix downloading the HTML file instead of previewing when
        # 'webpage_view' is enabled
        toolkit.add_template_directory(config_, 'theme/templates')

    # IConfigurable

    def configure(self, config):
        # Certain config options must exists for the plugin to work. Raise an
        # exception if they're missing.
        missing_config = "{0} is not configured. Please amend your .ini file."
        config_options = (
            'ckanext.s3filestore.aws_bucket_name',
            'ckanext.s3filestore.region_name',
            'ckanext.s3filestore.signature_version'
        )

        if not config.get('ckanext.s3filestore.aws_use_ami_role'):
            config_options += ('ckanext.s3filestore.aws_access_key_id',
                               'ckanext.s3filestore.aws_secret_access_key')

        for option in config_options:
            if not config.get(option, None):
                raise RuntimeError(missing_config.format(option))

        # Check that options actually work, if not exceptions will be raised
        if toolkit.asbool(
                config.get('ckanext.s3filestore.check_access_on_startup',
                           True)):
            ckanext.s3filestore.uploader.BaseS3Uploader().get_s3_bucket(
                config.get('ckanext.s3filestore.aws_bucket_name'))

    # IUploader

    def get_resource_uploader(self, data_dict):
        '''Return an uploader object used to upload resource files.'''
        return ckanext.s3filestore.uploader.S3ResourceUploader(data_dict)

    def get_uploader(self, upload_to, old_filename=None):
        '''Return an uploader object used to upload general files.'''
        return ckanext.s3filestore.uploader.S3Uploader(upload_to,
                                                       old_filename)

    # IBlueprint

    def get_blueprint(self):
        blueprints = resource.get_blueprints() +\
                     uploads.get_blueprints()
        return blueprints

    # IClick

    def get_commands(self):
        return [upload_resources, upload_assets]
    
    # IResourceController

    def before_resource_create(self, context, resource_dict):
        if aws_bucket_url in resource_dict['url']:
            resource_dict['url_bucket'] = resource_dict['url']
        
        return resource_dict

    def before_resource_show(self, resource_dict):
        
        if aws_bucket_url in resource_dict['url']:

            key_path = resource_dict['url'].replace(aws_bucket_url, "")
            url_signed = sign_url(key_path)

            resource_dict['url'] = url_signed
            return resource_dict
        
        m = re.search('Amz-Date=(.+?)&X-Amz-Expires', resource_dict['url'])
        
        if m:
            time = m.group(1)
            if sigiture_expired(time) is True:
                key_path = resource_dict['url_bucket'].replace(aws_bucket_url, "")
                url_signed = sign_url(key_path)
                resource_dict['url'] = url_signed
                return resource_dict
            else:
                return resource_dict
        else:
            return resource_dict

    

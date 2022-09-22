from io import StringIO
import os
import click

from sqlalchemy import create_engine
from sqlalchemy.sql import text
from ckantoolkit import config
from ckanext.s3filestore.uploader import BaseS3Uploader


@click.command(u's3-upload',
               short_help=u'Uploads all resources '
                          u'from "ckan.storage_path"'
                          u' to the configured s3 bucket')
def upload_resources():
    storage_path = config.get('ckan.storage_path',
                              '/var/lib/ckan/default/resources')
    sqlalchemy_url = config.get('sqlalchemy.url',
                                'postgresql://user:pass@localhost/db')
    bucket_name = config.get('ckanext.s3filestore.aws_bucket_name')
    acl = config.get('ckanext.s3filestore.acl', 'public-read')
    resource_ids_and_paths = {}

    for root, dirs, files in os.walk(storage_path):
        if files:
            resource_id = root.split('/')[-2] + root.split('/')[-1] + files[0]
            resource_ids_and_paths[resource_id] = os.path.join(root, files[0])

    click.secho(
        'Found {0} resource files in '
        'the file system'.format(len(resource_ids_and_paths.keys())),
        fg=u'green',
        bold=True)

    engine = create_engine(sqlalchemy_url)
    connection = engine.connect()

    resource_ids_and_names = {}

    try:
        for resource_id, file_path in resource_ids_and_paths.items():
            resource = connection.execute(text('''
                   SELECT id, url, url_type
                   FROM resource
                   WHERE id = :id
               '''), id=resource_id)
            if resource.rowcount:
                _id, url, _type = resource.first()
                if _type == 'upload' and url:
                    file_name = url.split('/')[-1] if '/' in url else url
                    resource_ids_and_names[_id] = file_name.lower()
    finally:
        connection.close()
        engine.dispose()

    click.secho('{0} resources matched on the database'.format(
        len(resource_ids_and_names.keys())),
        fg=u'green',
        bold=True)

    uploader = BaseS3Uploader()
    s3_connection = uploader.get_s3_resource()

    uploaded_resources = []
    for resource_id, file_name in resource_ids_and_names.items():
        key = 'resources/{resource_id}/{file_name}'.format(
            resource_id=resource_id, file_name=file_name)
        s3_connection.Object(bucket_name, key)\
            .put(Body=open(resource_ids_and_paths[resource_id],
                           u'rb'),
                 ACL=acl)
        uploaded_resources.append(resource_id)
        click.secho(
            'Uploaded resource {0} ({1}) to S3'.format(resource_id,
                                                       file_name),
            fg=u'green',
            bold=True)

    click.secho(
        'Done, uploaded {0} resources to S3'.format(
            len(uploaded_resources)),
        fg=u'green',
        bold=True)


@click.command(u's3-test-connection', short_help=u'Test connection to AWS S3')
def upload_resources():
    bucket_name = config.get('ckanext.s3filestore.aws_bucket_name')
    acl = config.get('ckanext.s3filestore.acl', 'public-read')
    
    click.secho('Testing AWS S3 connection', fg=u'green', bold=True)

    uploader = BaseS3Uploader()
    s3_connection = uploader.get_s3_resource()
    buf = StringIO('Testing S3 connection')

    try:
        s3_object = s3_connection.Object(bucket_name, 'testNN.text')

        s3_object.put(
            Body=buf.getvalue(),
            ACL=acl,
            ContentType='text/plain'
        )
    except Exception as e:
        error = 'Failed to create S3 Object: {}'.format(e)
        click.secho(error, fg=u'red', bold=True)
        raise click.Abort()

    click.secho('S3 Object created', fg=u'green', bold=True)
    s3_object.delete()
    click.secho('S3 Object deleted', fg=u'green', bold=True)

    click.secho('Done', fg=u'green', bold=True)

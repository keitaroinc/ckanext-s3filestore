# encoding: utf-8
u'''Tests for assuming an IAM role via STS when building the S3 session.

See https://github.com/keitaroinc/ckanext-s3filestore/issues/11
'''
from unittest import mock
import pytest

from ckanext.s3filestore.uploader import BaseS3Uploader


ROLE_ARN = u'arn:aws:iam::123456789012:role/RoleName'

TEMP_CREDENTIALS = {
    u'Credentials': {
        u'AccessKeyId': u'temp-access-key',
        u'SecretAccessKey': u'temp-secret-key',
        u'SessionToken': u'temp-session-token',
    }
}


def _session_for_role(role):
    u'''Build a session with the given aws_role, mocking out STS.

    Returns the (session, sts_client) pair so callers can assert both on the
    credentials that ended up in the session and on whether STS was called.
    '''
    uploader = BaseS3Uploader()
    uploader.role = role

    sts_client = mock.Mock()
    sts_client.assume_role.return_value = TEMP_CREDENTIALS

    with mock.patch(u'boto3.session.Session.client',
                    return_value=sts_client):
        session = uploader.get_s3_session()

    return session, sts_client


@pytest.mark.usefixtures(u'ckan_config')
class TestAssumeRole(object):

    def test_role_is_assumed_when_configured(self):
        u'''With aws_role set, STS is called and the session uses the
        temporary credentials it returns.'''

        session, sts_client = _session_for_role(ROLE_ARN)

        sts_client.assume_role.assert_called_once_with(
            RoleArn=ROLE_ARN,
            RoleSessionName=u'CkanExtS3Session')

        credentials = session.get_credentials()
        assert credentials.access_key == u'temp-access-key'
        assert credentials.secret_key == u'temp-secret-key'
        assert credentials.token == u'temp-session-token'

    def test_role_is_not_assumed_when_absent(self):
        u'''With no aws_role, STS is never called and the configured access
        key is used, preserving the behaviour from before this option.'''

        session, sts_client = _session_for_role(None)

        assert not sts_client.assume_role.called

        credentials = session.get_credentials()
        assert credentials.access_key == u'test-access-key'
        assert credentials.token is None

    @pytest.mark.parametrize(u'role', [u'', u'   ', u'\t'])
    def test_role_is_not_assumed_when_blank(self, role):
        u'''A blank or whitespace-only aws_role is treated as unset rather
        than passed to STS as an ARN.'''

        session, sts_client = _session_for_role(role)

        assert not sts_client.assume_role.called
        assert session.get_credentials().token is None

    def test_role_arn_is_stripped(self):
        u'''Surrounding whitespace in the configured ARN is ignored.'''

        _, sts_client = _session_for_role(u'  {0}  '.format(ROLE_ARN))

        sts_client.assume_role.assert_called_once_with(
            RoleArn=ROLE_ARN,
            RoleSessionName=u'CkanExtS3Session')

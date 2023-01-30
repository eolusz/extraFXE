import requests
from datetime import datetime
import os
import builtins
import re
import urllib
class Logbook():
    subject = "Robot post"
    _type = "Commissioning"
    category = "Robot"
    text = "Text"
    msgId = 1960
    _url ="https://in.xfel.eu/elog/FXE+Commissioning+%26+Operation/"

    def __init__(self, user="fxe_robot", passw="iamrobot", kwargs={}):
        self._user = user
        self._password = passw
        self.session=requests.Session()
        self.session.get(self._url, params={"uname":self._user, "upassword": self._password})
    
    def post(self):
        self.msgId = self._post(self.text, attributes={"Type": self.type, "Subject": self.subject, "Category": self.category})

    def edit(self):
        msg, atr, attac = self._read(self.msgId)
        self._post(msg + self.text, msg_id=self.msgId)

    def editTable(self):
        msg, atr, attac = self._read(self.msgId)
        tend = msg.index("\t</tbody>")
        txt=msg[:tend] + self.text +"\n" + msg[tend:]
        print(txt)
        self._post(txt,
                   msg_id=self.msgId)

    def read(self):
        self._read(self.msgId)
 
    def _make_user_and_pswd_cookie(self):
        """
        prepares user name and password cookie. It is sent in header when posting a message.
        :return: user name and password value for the Cookie header
        """
        cookie = ''
        if self._user:
            cookie += 'uname=' + self._user + ';'
        if self._password:
            cookie += 'upassword=' + self._password + ';'

        return cookie

    def _add_base_msg_attributes(self, data):
        """
        Adds base message attributes which are used by all messages.
        :param data: dict of current attributes
        :return: content string
        """
        data['cmd'] = 'Submit'
        #data['exp'] = self.logbook
        data['suppress']= 1

    def _read(self, msg_id):
        """
        Reads message from the logbook server and returns tuple of (message, attributes, attachments) where:
        message: string with message body
        attributes: dictionary of all attributes returned by the logbook
        attachments: list of urls to attachments on the logbook server
        :param msg_id: ID of the message to be read
        :return: message, attributes, attachments
        """
        #request_headers = dict()
        #if self._user or self._password:
        #    request_headers['Cookie'] = self._make_user_and_pswd_cookie()

        try:
            self._check_if_message_on_server(msg_id)  # raises exceptions if no message or no response from server
            response = self.session.get(self._url + str(msg_id) + '?cmd=download',
                                    allow_redirects=False, verify=False)

            # Validate response. If problems Exception will be thrown.
            resp_message, resp_headers, resp_msg_id = _validate_response(response)

        except RequestException as e:
            # If here: message is on server but cannot be downloaded (should never happen)
            raise LogbookServerProblem('Cannot access logbook server to read the message with ID: ' + str(msg_id) +
                                       'because of:\n' + '{0}'.format(e))

        # Parse message to separate message body, attributes and attachments
        attributes = dict()
        attachments = list()

        returned_msg = resp_message.decode('utf-8', 'ignore').splitlines()
        delimiter_idx = returned_msg.index('========================================')


        message = '\n'.join(returned_msg[delimiter_idx + 1:])
        for line in returned_msg[0:delimiter_idx]:
            line = line.split(': ')
            data = ''.join(line[1:])
            if line[0] == 'Attachment':
                if not data:
                    # Treat the empty string as special case,
                    # otherwise the split below returns [""] and attachments is [self._url]
                    attachments = []
                else:
                    attachments = data.split(',')
                    # Here are only attachment names, make a full url out of it, so they could be
                    # recognisable by others, and downloaded if needed
                    attachments = [self._url + '{0}'.format(i) for i in attachments]
            else:
                attributes[line[0]] = data

        return message, attributes, attachments
      
    def _post(self, message, msg_id=None, reply=False, 
              attributes=None, attachments=None, 
              encoding='HTML',  **kwargs):

        attributes = attributes or {}
        attributes = {**attributes, **kwargs}  # kwargs as attributes with higher priority

        attachments = attachments or []

        if encoding is not None:
            if encoding not in ['plain', 'HTML', 'ELCode']:
                raise LogbookMessageRejected('Invalid message encoding. Valid options: plain, HTML, ELCode.')
            attributes['Encoding'] = encoding

        attributes_to_edit = dict()
        if msg_id:
            # Message exists, we can continue
            if reply:
                # Verify that there is a message on the server, otherwise do not reply to it!
                self._check_if_message_on_server(msg_id)  # raises exception in case of none existing message

                attributes['reply_to'] = str(msg_id)

            else:  # Edit existing
                attributes['edit_id'] = str(msg_id)
                attributes['skiplock'] = '1'

                # Handle existing attachments
                msg_to_edit, attributes_to_edit, attach_to_edit = self._read(msg_id)

                i = 0
                for attachment in attach_to_edit:
                    if attachment:
                        # Existing attachments must be passed as regular arguments attachment<i> with value= file name
                        # Read message returnes full urls to existing attachments:
                        # <hostname>:[<port>][/<subdir]/<logbook>/<msg_id>/<file_name>
                        attributes['attachment' + str(i)] = os.path.basename(attachment)
                        i += 1

                for attribute, data in attributes.items():
                    new_data = attributes.get(attribute)
                    if new_data is not None:
                        attributes_to_edit[attribute] = new_data
        else:
            # As we create a new message, specify creation time if not already specified in attributes
            if 'When' not in attributes:
                attributes['When'] = int(datetime.now().timestamp())

        if not attributes_to_edit:
            attributes_to_edit = attributes
        # Remove any attributes that should not be sent
        _remove_reserved_attributes(attributes_to_edit)

        if attachments:
            files_to_attach, objects_to_close = self._prepare_attachments(attachments)
        else:
            objects_to_close = list()
            files_to_attach = list()

        # Make requests module think that Text is a "file". This is the only way to force requests to send data as
        # multipart/form-data even if there are no attachments. Elog understands only multipart/form-data
        files_to_attach.append(('Text', ('', message)))

        # Base attributes are common to all messages
        self._add_base_msg_attributes(attributes_to_edit)
        
        # Keys in attributes cannot have certain characters like whitespaces or dashes for the http request
        attributes_to_edit["Text"]= message
        attributes_to_edit = _replace_special_characters_in_attribute_keys(attributes_to_edit)

        try:
            response = self.session.post(self._url, data=attributes_to_edit, files=files_to_attach, allow_redirects=False, verify=False)
            #response = self.session.get(self._url, params=attributes_to_edit)
            # Validate response. Any problems will raise an Exception.
            resp_message, resp_headers, resp_msg_id = _validate_response(response)
            print(resp_message)

            # Close file like objects that were opened by the elog (if  path
            for file_like_object in objects_to_close:
                if hasattr(file_like_object, 'close'):
                    file_like_object.close()

        except requests.RequestException as e:
            # Check if message on server.
            self._check_if_message_on_server(msg_id)  # raises exceptions if no message or no response from server

            # If here: message is on server but cannot be downloaded (should never happen)
            raise LogbookServerProblem('Cannot access logbook server to post a message, ' + 'because of:\n' +
                                       '{0}'.format(e))

        # Any error before here should raise an exception, but check again for nay case.
        if not resp_msg_id or resp_msg_id < 1:
            raise LogbookInvalidMessageID('Invalid message ID: ' + str(resp_msg_id) + ' returned')
        return resp_msg_id
    
    def _check_if_message_on_server(self, msg_id):
        """Try to load page for specific message. If there is a htm tag like <td class="errormsg"> then there is no
        such message.
        :param msg_id: ID of message to be checked
        :return:
        """

        #request_headers = dict()
        #if self._user or self._password:
        #    request_headers['Cookie'] = self._make_user_and_pswd_cookie()
        try:
            response = self.session.get(self._url + str(msg_id), allow_redirects=False,
                                    verify=False)

            # If there is no message code 200 will be returned (OK) and _validate_response will not recognise it
            # but there will be some error in the html code.
            resp_message, resp_headers, resp_msg_id = _validate_response(response)
            # If there is no message, code 200 will be returned (OK) but there will be some error indication in
            # the html code.
            if re.findall('<td.*?class="errormsg".*?>.*?</td>',
                          resp_message.decode('utf-8', 'ignore'),
                          flags=re.DOTALL):
                raise LogbookInvalidMessageID('Message with ID: ' + str(msg_id) + ' does not exist on logbook.')

        except RequestException as e:
            raise LogbookServerProblem('No response from the logbook server.\nDetails: ' + '{0}'.format(e))
            
    def _prepare_attachments(self, files):
        """
        Parses attachments to content objects. Attachments can be:
            - file like objects: must have method read() which returns bytes. If it has attribute .name it will be used
              for attachment name, otherwise generic attribute<i> name will be used.
            - path to the file on disk
        Note that if attachment is is an url pointing to the existing Logbook server it will be ignored and no
        exceptions will be raised. This can happen if attachments returned with read_method are resend.
        :param files: list of file like objects or paths
        :return: content string
        """
        prepared = list()
        i = 0
        objects_to_close = list()  # objects that are created (opened) by elog must be later closed
        for file_obj in files:
            if hasattr(file_obj, 'read'):
                i += 1
                attribute_name = 'attfile' + str(i)

                filename = attribute_name  # If file like object has no name specified use this one
                candidate_filename = os.path.basename(file_obj.name)

                if filename:  # use only if not empty string
                    filename = candidate_filename

            elif isinstance(file_obj, str):
                # Check if it is:
                #           - a path to the file --> open file and append
                #           - an url pointing to the existing Logbook server --> ignore

                filename = ""
                attribute_name = ""

                if os.path.isfile(file_obj):
                    i += 1
                    attribute_name = 'attfile' + str(i)

                    file_obj = builtins.open(file_obj, 'rb')
                    filename = os.path.basename(file_obj.name)

                    objects_to_close.append(file_obj)

                elif not file_obj.startswith(self._url):
                    raise LogbookInvalidAttachmentType('Invalid type of attachment: \"' + file_obj + '\".')
            else:
                raise LogbookInvalidAttachmentType('Invalid type of attachment[' + str(i) + '].')

            prepared.append((attribute_name, (filename, file_obj)))

        return prepared, objects_to_close




def _remove_reserved_attributes(attributes):
    """
    Removes elog reserved attributes (from the attributes dict) that can not be sent.

    :param attributes: dictionary of attributes to be cleaned.
    :return:
    """

    if attributes:
        attributes.get('$@MID@$', None)
        attributes.pop('Date', None)
        attributes.pop('Attachment', None)
        attributes.pop('Text', None)  # Remove this one because it will be send attachment like

def _replace_special_characters_in_attribute_keys(attributes):
    """
    Replaces special characters in elog attribute keys by underscore, otherwise attribute values will be erased in
    the http request. This is using the same replacement elog itself is using to handle these cases
    :param attributes: dictionary of attributes to be cleaned.
    :return: attributes with replaced keys
    """
    return {re.sub('[^0-9a-zA-Z]', '_', key): value for key, value in attributes.items()}

def _validate_response(response):
    """ Validate response of the request."""

    msg_id = None

    if response.status_code not in [200, 302]:
        # 200 --> OK; 302 --> Found
        # Html page is returned with error description (handling errors same way as on original client. Looks
        # like there is no other way.

        err = re.findall('<td.*?class="errormsg".*?>.*?</td>',
                         response.content.decode('utf-8', 'ignore'),
                         flags=re.DOTALL)

        if len(err) > 0:
            # Remove html tags
            # If part of the message has: Please go  back... remove this part since it is an instruction for
            # the user when using browser.
            err = re.sub('(?:<.*?>)', '', err[0])
            if err:
                raise LogbookMessageRejected('Rejected because of: ' + err)
            else:
                raise LogbookMessageRejected('Rejected because of unknown error.')

        # Other unknown errors
        raise LogbookMessageRejected('Rejected because of unknown error.')
    else:
        location = response.headers.get('Location')
        if location is not None:
            if 'has moved' in location:
                raise LogbookServerProblem('Logbook server has moved to another location.')
            elif 'fail' in location:
                raise LogbookAuthenticationError('Invalid username or password.')
            else:
                # returned locations is something like: '<host>/<sub_dir>/<logbook>/<msg_id><query>
                # with urllib.parse.urlparse returns attribute path=<sub_dir>/<logbook>/<msg_id>
                print(urllib.parse.urlsplit(location).path.split('/')[-1])
                msg_id = int(urllib.parse.urlsplit(location).path.split('/')[-1])

        if b'form name=form1' in response.content or b'type=password' in response.content:
            # Not to smart to check this way, but no other indication of this kind of error.
            # C client does it the same way
            raise LogbookAuthenticationError('Invalid username or password.')

    return response.content, response.headers, msg_id

class LogbookError(Exception):
    """ Parent logbook exception."""
    pass


class LogbookAuthenticationError(LogbookError):
    """ Raise when problem with username and password."""
    pass


class LogbookServerProblem(LogbookError):
    """ Raise when problem accessing logbook server."""
    pass


class LogbookMessageRejected(LogbookError):
    """ Raised when manipulating/creating message was rejected by the server or there was problem composing message."""
    pass


class LogbookInvalidMessageID(LogbookMessageRejected):
    """ Raised when there is no message with specified ID on the server."""
    pass


class LogbookInvalidAttachmentType(LogbookMessageRejected):
    """ Raised when passed attachment has invalid type."""
    pass
        

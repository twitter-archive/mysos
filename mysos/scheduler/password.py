import random
import string

import nacl.exceptions
import nacl.secret
import nacl.utils


class PasswordBox(object):
  """
    Implements password encryption using PyNaCl.
  """

  class Error(Exception): pass

  def __init__(self, key):
    self._secret_box = nacl.secret.SecretBox(key)

  def encrypt(self, plaintext):
    try:
      return self._secret_box.encrypt(
          plaintext, nacl.utils.random(nacl.secret.SecretBox.NONCE_SIZE))
    except nacl.exceptions.CryptoError as e:
      raise self.Error("Failed to encrypt the password: %s" % e)

  def decrypt(self, encrypted):
    try:
      return self._secret_box.decrypt(encrypted)
    except nacl.exceptions.CryptoError as e:
      raise self.Error("Failed to decrypt the password: %s" % e)

  def match(self, plaintext, encrypted):
    return plaintext == self._secret_box.decrypt(encrypted)


def gen_password():
  """Return a randomly-generated password of 21 characters."""
  return ''.join(random.choice(
      string.ascii_uppercase +
      string.ascii_lowercase +
      string.digits) for _ in range(21))


def gen_encryption_key():
  """Return a randomly-generated encryption key of 32 characters."""
  return nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE)

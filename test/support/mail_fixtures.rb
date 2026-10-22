# frozen_string_literal: true

# One rendered message for both delivery methods: every recipient group is
# populated, and the display-name sender exercises the From/envelope split.
module MailFixtures
  def build_message
    Mail.new do
      from 'Alice Example <alice@example.com>'
      to 'to@example.com'
      cc 'cc@example.com'
      bcc 'bcc@example.com'
      subject 'Delivery parity'
      body 'Hello from Rails.'
    end
  end
end

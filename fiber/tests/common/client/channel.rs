use dope_amqp091::adapter::publish;
use dope_amqp091::message::property::Properties;

pub fn request<'exchange, 'key, 'props, 'body>(
    exchange: &'exchange str,
    key: &'key str,
    props: &'props Properties,
    body: &'body [u8],
) -> publish::Request<'exchange, 'key, 'props, 'body> {
    publish::Request {
        exchange,
        routing_key: key,
        mandatory: false,
        properties: props,
        body,
    }
}

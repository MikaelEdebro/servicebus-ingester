-- name: InsertMessage :exec
INSERT INTO messages (message_id, event_type, source, body)
VALUES (@message_id, @event_type, @source, @body);

-- name: GetMessageByID :one
SELECT * FROM messages WHERE id = @id;

-- name: ListMessagesByEventType :many
SELECT * FROM messages WHERE event_type = @event_type ORDER BY created_at DESC LIMIT @msg_limit;

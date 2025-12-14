PostText: str = JsonData(
  path='$.operation.record.text',
  required=False,
  coerce_type=True,
)

PostReplyParent: Entity[str] = EntityJson(
  type='AtUri',
  path='$.operation.record.reply.parent.uri',
  required=False,
)

PostReplyRoot: Entity[str] = EntityJson(
  type='AtUri',
  path='$.operation.record.reply.root.uri',
  required=False,
)

PostIsReply = PostReplyParent != None and PostReplyRoot != None

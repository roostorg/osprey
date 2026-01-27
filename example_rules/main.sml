Import(rules=['models/base.sml'])

# Original demo rule
Require(rule='rules/post_contains_hello.sml')

# Conference demo rules
Require(rule='rules/spam_link_detection.sml')
Require(rule='rules/identity_change_detection.sml')
Require(rule='rules/new_account_patterns.sml')
Require(rule='rules/ip_metadata.sml')

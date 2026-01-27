# Osprey Conference Demo Guide

This guide covers the demo data and rules for showcasing Osprey's rule engine capabilities.

## Prerequisites

Ensure Osprey is running via Docker:

```bash
# Start Osprey services
docker compose up -d

# Verify services are running
docker ps | grep osprey
```

You should see: `osprey-ui`, `osprey-ui-api`, `osprey-worker`, `osprey-kafka`

## Quick Start

```bash
# Start the demo data generator (uses Docker by default)
./example_data/generate_test_data.sh

# The script will continuously generate events until you press Ctrl+C
```

Access the UI at: **http://localhost:5002**

## Event Types Generated

The demo generates three types of events with ~20% containing malicious patterns:

| Event Type | Frequency | Description |
|------------|-----------|-------------|
| `create_post` | 50% | Posts with text, embeds, mentions, and URLs |
| `send_message` | 30% | Direct messages between users |
| `identity_change` | 20% | Handle and display name changes |

## Detection Rules

### 1. Spam Link Detection (`spam_link_detection.sml`)

Detects:
- Spam links in messages (bit.ly, tinyurl, click-here, etc.)
- Spam links in posts
- Crypto scam patterns
- New accounts sending links

**Labels applied:** `spam_sender`, `spam_poster`, `suspicious_new_account`

### 2. Identity Change Detection (`identity_change_detection.sml`)

Detects:
- Handle changes to impersonate authority (admin, support, moderator)
- Rapid handle changes from new accounts
- Suspicious display names (FREE, GIVEAWAY, etc.)
- Identity wipe patterns (changing to generic handles)

**Labels applied:** `impersonation_attempt`, `identity_evasion`, `spam_display_name`

### 3. New Account Patterns (`new_account_patterns.sml`)

Detects:
- Bot-like posting (high posts, few followers)
- Mass messaging from new accounts
- Automated user agents (curl, python-requests, etc.)
- New accounts posting links
- Low engagement ratios

**Labels applied:** `suspected_bot`, `mass_messaging`, `automated_client`, `new_account_links`

### 4. IP/Metadata Detection (`ip_metadata.sml`)

Detects:
- VPN/proxy IP ranges
- Headless browsers (Selenium, Puppeteer, PhantomJS)
- User agent spoofing
- Missing/invalid user agents
- New accounts from suspicious IPs

**Labels applied:** `proxy_user`, `automation_detected`, `suspicious_client`

## Data Models

### Base Models (`models/base.sml`)
- `UserId` - User identifier
- `EventType` - Type of event
- `ActionName` - Action being performed

### Identity Models (`models/identity.sml`)
- `PreviousHandle` - Handle before change
- `NewHandle` - Handle after change
- `DisplayName` - User's display name
- `ChangeType` - Type of identity change

### Message Models (`models/message.sml`)
- `MessageText` - Direct message content
- `RecipientId` - Message recipient

### Account Models (`models/account.sml`)
- `AccountCreatedAt` - Account creation timestamp
- `PostCount` - Number of posts
- `FollowerCount` - Number of followers

### Metadata Models (`models/metadata.sml`)
- `IpAddress` - Client IP address
- `UserAgent` - Client user agent string
- `IpNetworkValue` - IP network for range checks

### Post Models (`models/post.sml`)
- `PostText` - Post content
- `EmbedUrls` - Embedded URLs
- `Mentions` - User mentions
- `ExtractedUrls` - URLs extracted from text

## Demo Scenarios

### Scenario 1: Spam Detection
Watch for events with `bit.ly`, `crypto`, or `guaranteed returns` in message text. These trigger spam detection rules and apply labels.

### Scenario 2: Bot Detection
Look for accounts with high `post_count` (>50) but low `follower_count` (<5). These trigger bot detection rules.

### Scenario 3: Impersonation
Watch for identity changes where `new_handle` contains `admin`, `support`, or `moderator`. These trigger impersonation detection.

### Scenario 4: Automation Detection
Look for events with user agents containing `python-requests`, `curl`, or `Headless`. These trigger automation detection.

## Using the UI

### Query Examples

**Important:** Use Python-style capitalized booleans (`True`/`False`), not lowercase.

```
# Find spam display names
SuspiciousDisplayName == True

# Find posts with spam links
PostContainsSpamLink == True

# Find automated user agents
AutomatedUserAgent == True

# Find VPN/proxy users
VpnProxyDetection == True

# Combine filters
SuspiciousDisplayName == True and PostCount < 10
```

### Top N Analysis

1. Set your query filter (e.g., `SuspiciousDisplayName == True`)
2. Choose a dimension for Top N (e.g., `DisplayName`, `PostText`, `UserId`)
3. View the breakdown of flagged items

## Configuration

Environment variables for the data generator:

```bash
# Docker mode (default) - uses Docker container to send to Kafka
USE_DOCKER=true
KAFKA_BROKER=osprey-kafka:29092
KAFKA_TOPIC=osprey.actions_input

# External Kafka mode
USE_DOCKER=false
KAFKA_BROKER=your-kafka-host:9092
KAFKA_TOPIC=osprey.actions_input
KAFKA_CONFIG_FILE=/path/to/producer.config  # Optional
```

## File Structure

```
example_data/
├── generate_test_data.sh      # Main demo data generator
└── DEMO_GUIDE.md              # This file

example_rules/
├── main.sml                   # Entry point
├── models/
│   ├── base.sml              # Base entities
│   ├── identity.sml          # Identity change entities
│   ├── message.sml           # Message entities
│   ├── account.sml           # Account entities
│   ├── metadata.sml          # IP/UA entities
│   └── post.sml              # Post entities (updated)
├── rules/
│   ├── post_contains_hello.sml      # Original demo
│   ├── spam_link_detection.sml      # Spam detection
│   ├── identity_change_detection.sml # Identity detection
│   ├── new_account_patterns.sml     # Bot/account patterns
│   └── ip_metadata.sml              # IP/metadata detection
└── config/
    └── labels.yaml           # Label definitions
```

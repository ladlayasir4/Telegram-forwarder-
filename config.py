# PrivateBot Configuration - Enhanced Multi-Channel Forwarding System

# Telethon API credentials
API_ID = 24476061
API_HASH = "ece295f5fb7d8eb9af706dc4a1e271f2"
PHONE = "+923422594385"

# Multiple source channels with their respective target channels
# Format: {source_id: [target1, target2, ...]}
CHANNEL_MAPPING = {
    -1002542096593: [-1002699485338, -1002777769439],  # Source1 -> Target1, Target2
    -1002150119396: [-1002777769439],  # Source2 -> Target1
    "@public_channel": ["@another_channel"]  # Public channel support
}

# Database path for storing forwarded message IDs
DB_PATH = "forwarded_messages.db"

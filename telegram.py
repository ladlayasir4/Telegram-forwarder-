#!/usr/bin/env python3
# Enhanced Telegram Auto-Forwarder with Multi-Source/Multi-Target Support

import os
import time
import random
import asyncio
import aiosqlite
from datetime import datetime
from telethon import TelegramClient, events
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.errors import FloodWaitError, SlowModeWaitError, FloodError
import config

print("Starting Enhanced Telegram Auto-Forwarder...")

# Create a Telegram client
client = TelegramClient('user_session', config.API_ID, config.API_HASH)

# Database initialization
async def init_db():
    """Initialize the SQLite database to track forwarded messages"""
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS forwarded_messages (
                source_id INTEGER,
                message_id INTEGER,
                target_id INTEGER,
                forward_time TIMESTAMP,
                PRIMARY KEY (source_id, message_id, target_id)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS last_processed (
                source_id INTEGER PRIMARY KEY,
                last_message_id INTEGER
            )
        ''')
        await db.commit()

async def is_message_forwarded(source_id, message_id, target_id):
    """Check if a message has already been forwarded to a target"""
    async with aiosqlite.connect(config.DB_PATH) as db:
        cursor = await db.execute(
            'SELECT 1 FROM forwarded_messages WHERE source_id = ? AND message_id = ? AND target_id = ?',
            (source_id, message_id, target_id)
        )
        result = await cursor.fetchone()
        return result is not None

async def mark_message_forwarded(source_id, message_id, target_id):
    """Mark a message as forwarded to a target"""
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(
            'INSERT INTO forwarded_messages (source_id, message_id, target_id, forward_time) VALUES (?, ?, ?, ?)',
            (source_id, message_id, target_id, datetime.now())
        )
        await db.commit()

async def get_last_processed_id(source_id):
    """Get the last processed message ID for a source"""
    async with aiosqlite.connect(config.DB_PATH) as db:
        cursor = await db.execute(
            'SELECT last_message_id FROM last_processed WHERE source_id = ?',
            (source_id,)
        )
        result = await cursor.fetchone()
        return result[0] if result else 0

async def update_last_processed_id(source_id, message_id):
    """Update the last processed message ID for a source"""
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(
            'INSERT OR REPLACE INTO last_processed (source_id, last_message_id) VALUES (?, ?)',
            (source_id, message_id)
        )
        await db.commit()

async def forward_message_with_retry(source_entity, message, target_entity, max_retries=3):
    """Forward a message with retry logic and rate limit handling"""
    retry_count = 0
    
    # Skip service messages
    from telethon.tl.patched import MessageService
    if isinstance(message, MessageService):
        print("Skipping service message (cannot be forwarded)")
        return False
    
    # Check if already forwarded
    if await is_message_forwarded(source_entity.id, message.id, target_entity.id):
        print(f"Message {message.id} already forwarded to target {target_entity.id}, skipping")
        return True
    
    while retry_count <= max_retries:
        try:
            print(f"Forwarding message {message.id} to target {target_entity.id}...")
            await client.forward_messages(target_entity, message)
            
            # Mark as forwarded
            await mark_message_forwarded(source_entity.id, message.id, target_entity.id)
            print("Message forwarded successfully!")
            return True
            
        except (FloodWaitError, FloodError, SlowModeWaitError) as e:
            wait_time = e.seconds
            print(f"Rate limited! Waiting {wait_time} seconds...")
            await asyncio.sleep(wait_time + random.randint(5, 15))
            retry_count += 1
            
        except Exception as e:
            if "A wait of" in str(e) and "seconds is required" in str(e):
                import re
                match = re.search(r'A wait of (\d+) seconds is required', str(e))
                if match:
                    wait_time = int(match.group(1))
                    print(f"Rate limited! Waiting {wait_time} seconds...")
                    await asyncio.sleep(wait_time + random.randint(5, 15))
                    retry_count += 1
                else:
                    print(f"Error forwarding message: {str(e)}")
                    return False
            else:
                print(f"Error forwarding message: {str(e)}")
                return False
    
    print(f"Failed to forward message {message.id} after {max_retries} retries")
    return False

async def process_source_channel(source_id, target_ids):
    """Process messages from a source channel to multiple target channels"""
    print(f"Processing source channel {source_id}...")
    
    try:
        source_entity = await client.get_entity(source_id)
        print(f"Successfully connected to source channel: {source_entity.title}")
    except Exception as e:
        print(f"Error accessing source channel {source_id}: {str(e)}")
        return
    
    # Get target entities
    target_entities = []
    for target_id in target_ids:
        try:
            target_entity = await client.get_entity(target_id)
            target_entities.append(target_entity)
            print(f"Successfully connected to target channel: {target_entity.title}")
        except Exception as e:
            print(f"Error accessing target channel {target_id}: {str(e)}")
    
    if not target_entities:
        print(f"No valid target channels for source {source_id}, skipping")
        return
    
    # Get last processed message ID
    last_message_id = await get_last_processed_id(source_id)
    print(f"Last processed message ID for source {source_id}: {last_message_id}")
    
    # Fetch messages from the source channel
    print(f"Fetching messages from source channel {source_id}...")
    
    offset_id = 0
    all_messages = []
    limit = 100
    
    while True:
        try:
            history = await client(GetHistoryRequest(
                peer=source_entity,
                offset_id=offset_id,
                offset_date=None,
                add_offset=0,
                limit=limit,
                max_id=0,
                min_id=0,
                hash=0
            ))
            
            if not history.messages:
                break
                
            messages = history.messages
            # Only process messages newer than the last processed one
            if last_message_id > 0:
                messages = [m for m in messages if m.id > last_message_id]
                
            all_messages.extend(messages)
            
            if len(messages) < limit:
                break
                
            offset_id = messages[-1].id
            print(f"Fetched {len(all_messages)} messages so far...")
            
        except Exception as e:
            print(f"Error fetching messages: {str(e)}")
            break
    
    # Process messages from oldest to newest
    all_messages.sort(key=lambda m: m.id)
    
    print(f"Found {len(all_messages)} new messages. Starting to forward...")
    
    # Forward messages to all targets
    for i, message in enumerate(all_messages):
        for target_entity in target_entities:
            success = await forward_message_with_retry(source_entity, message, target_entity)
            
            if success:
                # Update last processed ID
                await update_last_processed_id(source_id, message.id)
            
            # Add delay between forwards to avoid rate limiting
            if i < len(all_messages) - 1:
                wait_time = random.randint(10, 20)
                print(f"Waiting {wait_time} seconds before next forward...")
                await asyncio.sleep(wait_time)
    
    print(f"Finished processing source channel {source_id}")

async def setup_event_handlers():
    """Set up event handlers for real-time message forwarding"""
    message_queue = asyncio.Queue()
    
    # Function to process messages from the queue
    async def process_message_queue():
        print("\nMessage queue processor started - Will process messages sequentially")
        
        while True:
            try:
                # Get message from queue
                source_entity, message = await message_queue.get()
                
                # Get targets for this source
                target_ids = config.CHANNEL_MAPPING.get(source_entity.id, [])
                
                # Process for each target
                for target_id in target_ids:
                    try:
                        target_entity = await client.get_entity(target_id)
                        await forward_message_with_retry(source_entity, message, target_entity)
                    except Exception as e:
                        print(f"Error processing target {target_id}: {str(e)}")
                
                # Mark task as done
                message_queue.task_done()
                
                # Add delay if queue has more messages
                if message_queue.qsize() > 0:
                    wait_time = random.randint(10, 15)
                    print(f"Waiting {wait_time} seconds before processing next message...")
                    await asyncio.sleep(wait_time)
                    
            except Exception as e:
                print(f"Error in queue processor: {str(e)}")
                await asyncio.sleep(5)
    
    # Start the queue processor
    asyncio.create_task(process_message_queue())
    
    # Set up event handlers for each source channel
    for source_id in config.CHANNEL_MAPPING.keys():
        try:
            source_entity = await client.get_entity(source_id)
            
            @client.on(events.NewMessage(chats=source_entity))
            async def handler(event):
                # Add message to queue for processing
                await message_queue.put((source_entity, event.message))
                print(f"New message from {source_entity.title} added to queue")
                
            print(f"Event handler set up for source: {source_entity.title}")
            
        except Exception as e:
            print(f"Error setting up event handler for source {source_id}: {str(e)}")
    
    return message_queue

async def main():
    # Initialize database
    await init_db()
    
    print("Connecting to Telegram...")
    await client.start(phone=config.PHONE)
    
    print("Processing historical messages from all sources...")
    # Process historical messages for each source
    for source_id, target_ids in config.CHANNEL_MAPPING.items():
        await process_source_channel(source_id, target_ids)
    
    print("\n--- Starting real-time monitoring for new messages ---")
    # Set up event handlers for real-time processing
    message_queue = await setup_event_handlers()
    
    print("Monitoring for new messages. Press Ctrl+C to stop.")
    
    # Keep the script running
    while True:
        await asyncio.sleep(300)
        print(f"Status: Queue size = {message_queue.qsize()}")
        
        # Periodic database maintenance (optional)
        async with aiosqlite.connect(config.DB_PATH) as db:
            # Clean up old records (older than 30 days)
            await db.execute(
                'DELETE FROM forwarded_messages WHERE forward_time < datetime("now", "-30 days")'
            )
            await db.commit()

# Run the main function
if __name__ == "__main__":
    try:
        with client:
            client.loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\nBot stopped by user.")
    except Exception as e:
        print(f"\nError: {str(e)}")

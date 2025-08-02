# event_bus.py
from collections import defaultdict

class EventBus:
    def __init__(self):
        self.subscribers = defaultdict(list)

    def subscribe(self, event_name, callback):
        self.subscribers[event_name].append(callback)

    def emit(self, event_name, data):
        for callback in self.subscribers.get(event_name, []):
            try:
                callback(data)
            except Exception as e:
                print(f"[EventBus] Erreur callback {event_name} : {e}")

# Global
event_bus = EventBus()
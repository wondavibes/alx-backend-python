from rest_framework.routers import DefaultRouter
from rest_framework_nested.routers import NestedDefaultRouter  # type: ignore

from .views import ConversationViewSet, MessageViewSet

# top-level router for conversations
router = DefaultRouter()
router.register(r"conversations", ConversationViewSet, basename="conversation")

# nested router to expose messages under a conversation
conversations_router = NestedDefaultRouter(
    router, r"conversations", lookup="conversation"
)
conversations_router.register(
    r"messages", MessageViewSet, basename="conversation-messages"
)

# expose both top-level and nested routes
urlpatterns = router.urls + conversations_router.urls

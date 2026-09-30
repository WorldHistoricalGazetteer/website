import logging

from django.contrib import admin
from django.contrib.auth import get_user_model

User = get_user_model()

logger = logging.getLogger("email_access")


class UserAdmin(admin.ModelAdmin):
    list_display = ('email', 'username', 'id', 'name', 'affiliation', 'role',)
    fields = ('id', 'username', 'email', 'name', 'affiliation', 'role', 'date_joined',
              'groups', ('is_staff', 'is_active', 'is_superuser'))
    readonly_fields = ('id', 'date_joined', 'last_login')
    list_filter = ('role',)

    filter_horizontal = ('groups',)
    search_fields = ('username', 'name')

    def changelist_view(self, request, extra_context=None):
        logger.info(f"[ADMIN] User list containing decrypted email addresses viewed by user '{request.user}'.")
        return super().changelist_view(request, extra_context=extra_context)

    def change_view(self, request, object_id, form_url='', extra_context=None):
        user = self.get_object(request, object_id)
        if user:
            logger.info(
                f"[ADMIN] User '{request.user.username}' "
                f"viewed decrypted email for user '{user.username}' "
                f"(ID: {user.id})."
            )
        return super().change_view(request, object_id, form_url, extra_context)


admin.site.register(User, UserAdmin)

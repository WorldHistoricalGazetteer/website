from django.contrib import admin
from django.contrib.contenttypes.admin import GenericTabularInline
from .models import Person, EmailAddress, Contribution


class ContributionInline(GenericTabularInline):
    """Edit CRediT contributions inline on the credited object's admin page.

    Reused by Dataset and Collection admin (and usable on any credited model).
    """
    model = Contribution
    extra = 1
    autocomplete_fields = ("person",)
    fields = ("person", "role", "degree", "is_corresponding", "order")

class EmailAddressInline(admin.TabularInline):
    model = Person.emails.through
    extra = 1

class PersonAdmin(admin.ModelAdmin):
    list_display = (
        'family',
        'given',
        'orcid',
        'affiliation',
        'email_list',
    )
    search_fields = (
        'family',
        'given',
        'orcid',
        'emails__address',
    )
    filter_horizontal = ('emails',)
    inlines = [EmailAddressInline]  # Add this line to include the inline

    def email_list(self, obj):
        return ', '.join(email.address for email in obj.emails.all())
    email_list.short_description = 'Emails'

admin.site.register(Person, PersonAdmin)

class EmailAddressAdmin(admin.ModelAdmin):
    list_display = ('address',)

admin.site.register(EmailAddress, EmailAddressAdmin)


@admin.register(Contribution)
class ContributionAdmin(admin.ModelAdmin):
    list_display = ('person', 'role', 'degree', 'is_corresponding',
                    'content_type', 'object_id', 'order')
    list_filter = ('role', 'degree', 'is_corresponding', 'content_type')
    search_fields = ('person__family', 'person__given', 'person__orcid', 'object_id')
    autocomplete_fields = ('person',)
    ordering = ('content_type', 'object_id', 'order')

from django.contrib import admin

from .models import (ContributionTerms, NewRuleProposal, PolicyAnswer,
                     PolicyQuestion, Review, ReviewerAgreement,
                     ReviewerCompetence, Rule, RuleSet, RuleSetVersion)


@admin.register(RuleSet)
class RuleSetAdmin(admin.ModelAdmin):
    list_display = ('slug', 'code', 'language_name', 'script_name', 'posture',
                    'corpus_name_count', 'conversion_rate', 'present_upstream')
    list_filter = ('posture', 'script_code', 'present_upstream')
    search_fields = ('slug', 'code', 'language_name', 'script_name')


@admin.register(RuleSetVersion)
class RuleSetVersionAdmin(admin.ModelAdmin):
    list_display = ('ruleset', 'blob_sha', 'row_count', 'fetched_at', 'is_current')
    list_filter = ('is_current',)


@admin.register(Rule)
class RuleAdmin(admin.ModelAdmin):
    list_display = ('ruleset', 'orth', 'current_ipa', 'corpus_frequency',
                    'review_count', 'lint_codes', 'present_upstream')
    list_filter = ('ruleset__posture', 'present_upstream', 'ruleset')
    search_fields = ('orth', 'current_ipa', 'ruleset__code')
    # Rules mirror the upstream file. Editing one here would put the database
    # out of step with the CSV until the next sync silently reverted it.
    readonly_fields = ('ruleset', 'orth', 'orth_source', 'current_ipa',
                       'current_ipa_source', 'row_index', 'lint_codes')


@admin.register(Review)
class ReviewAdmin(admin.ModelAdmin):
    list_display = ('rule', 'reviewer', 'verdict', 'proposed_ipa', 'reviewed_ipa',
                    'confidence', 'is_latest', 'adopted_upstream_at', 'created')
    list_filter = ('verdict', 'confidence', 'is_latest', 'rule__ruleset')
    search_fields = ('rule__orth', 'proposed_ipa', 'comment')
    # Append-only by design: disagreement is the signal being collected, and an
    # edit here would resolve it invisibly.
    readonly_fields = tuple(f.name for f in Review._meta.fields)

    def has_add_permission(self, request):
        return False


@admin.register(NewRuleProposal)
class NewRuleProposalAdmin(admin.ModelAdmin):
    list_display = ('ruleset', 'orth', 'proposed_ipa', 'proposer', 'confidence',
                    'status', 'is_latest', 'adopted_upstream_at', 'created')
    list_filter = ('status', 'is_latest', 'ruleset')
    search_fields = ('orth', 'proposed_ipa', 'comment')
    # `status` is the one editable field: declining a proposal is a decision
    # someone makes, but the proposal itself is the contributor's words.
    readonly_fields = tuple(f.name for f in NewRuleProposal._meta.fields
                            if f.name != 'status')

    def has_add_permission(self, request):
        return False


@admin.register(ReviewerCompetence)
class ReviewerCompetenceAdmin(admin.ModelAdmin):
    list_display = ('user', 'language_code', 'script_code', 'level', 'created')
    list_filter = ('level', 'language_code')


@admin.register(ContributionTerms)
class ContributionTermsAdmin(admin.ModelAdmin):
    list_display = ('version', 'licence_spdx', 'is_active', 'signed_off', 'created')
    list_filter = ('is_active', 'signed_off')


@admin.register(ReviewerAgreement)
class ReviewerAgreementAdmin(admin.ModelAdmin):
    list_display = ('user', 'terms', 'credit_name', 'credit_public', 'accepted_at')


class PolicyAnswerInline(admin.TabularInline):
    model = PolicyAnswer
    extra = 0
    readonly_fields = ('user', 'option_key', 'comment', 'competence_level', 'created', 'is_latest')


@admin.register(PolicyQuestion)
class PolicyQuestionAdmin(admin.ModelAdmin):
    list_display = ('title', 'ruleset', 'language_code', 'status', 'created')
    list_filter = ('status',)
    prepopulated_fields = {'slug': ('title',)}
    inlines = [PolicyAnswerInline]

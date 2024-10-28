from django import forms
from sop.models import SavedQuery

class SavedQueryForm(forms.ModelForm):
    class Meta:
        model = SavedQuery
        fields = ['title', 'db',  'sql_query', 'comment']

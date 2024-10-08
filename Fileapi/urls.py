from django.urls import path
from .views import FilePathUploadView

urlpatterns = [
    path('upload/', FilePathUploadView.as_view(), name='file-path-upload'),
]

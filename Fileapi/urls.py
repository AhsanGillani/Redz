from django.urls import path, include
from .views import FilePathUploadView
from .views import ProjectImageUploadView

urlpatterns = [
    path('upload/', FilePathUploadView.as_view(), name='file-path-upload'),
    path('api/', ProjectImageUploadView.as_view(), name='project-id'),
  
]


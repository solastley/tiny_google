from django.shortcuts import render
from django.http import HttpResponse
from django.conf import settings

import subprocess
import os

# Create your views here.
def index(request):
    context = {}
    return render(request, "index.html", context)

def initialize(request):
    # TODO: execute shell commands to initialize the inverted index here
    print "Initializing the inverted index..."
    path = settings.BASE_DIR + "/app/mapreduce/init.sh"
    p = subprocess.Popen(path, shell=True, stdout=subprocess.PIPE)
    p.wait()
    return HttpResponse(status=200)

def search(request):
    # TODO: execute shell commands to search the inverted index here
    print "Searching the inverted index..."
    return HttpResponse("Success!")

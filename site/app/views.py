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
    print "Initializing the inverted index..."
    if os.path.isfile("/tmp/tiny_google_index/index.txt"):
        return HttpResponse(status=200)
    path = settings.BASE_DIR + "/app/mapreduce/init.sh"
    p = subprocess.Popen(path, shell=True, stdout=subprocess.PIPE)
    p.wait()
    return HttpResponse(status=200)

def search(request):
    print "Searching the inverted index..."

    # generate the command to execute bash script using keywords
    keyword_string = request.POST["search"]
    keywords = keyword_string.split()
    command = settings.BASE_DIR + "/app/mapreduce/search.sh "
    for i in range(len(keywords)):
        command += keywords[i] + " "

    # execute bash script and wait for it to complete
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    p.wait()

    # read the results file
    f = open("/tmp/tiny_google_results/results.txt")
    output = f.read()
    f.close()

    return HttpResponse(output)

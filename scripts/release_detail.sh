#!/bin/bash
#
# Copyright 2026 Aiven Oy and project contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#        SPDX-License-Identifier: Apache-2.0
#
# Example usage: ./scripts/release_detail.sh v3.1.1 3.2.0

git fetch origin
if [ -z $2 ]
then
  echo "Must provide start tag and final version"
  exit 1
fi

startTag=${1}
endVersion=${2}

start=`git rev-parse ${startTag}`;
if [ ${start} == ${startTag} ]
then
  echo ${startTag} is not a valid git tag for this repository
  exit 1
fi

# Diff against the release tag (matching the GitHub compare link below),
# falling back to HEAD when the tag does not exist yet.
if git rev-parse --verify --quiet "v${endVersion}" > /dev/null; then
  end=`git rev-parse v${endVersion}`;
else
  end=`git rev-parse HEAD`;
fi
commits=${start}..${end};
echo '## v'${endVersion} > /tmp/proposed_changelog.txt;
echo '### What is changed' >> /tmp/proposed_changelog.txt;
echo '' >> /tmp/proposed_changelog.txt;
git log --format=' - %s'  ${commits} >> /tmp/proposed_changelog.txt;
echo '' >> /tmp/proposed_changelog.txt;
echo '' >> /tmp/proposed_changelog.txt;
echo '### Co-authored by' >> /tmp/proposed_changelog.txt;
echo '' >> /tmp/proposed_changelog.txt;
git log --format=' - %an'  ${commits} | sort -u  >> /tmp/proposed_changelog.txt;
echo '' >> /tmp/proposed_changelog.txt;
echo '' >> /tmp/proposed_changelog.txt;
echo '### Full Changelog' >> /tmp/proposed_changelog.txt;
echo 'https://github.com/Aiven-Open/opensearch-connector-for-apache-kafka/compare/'${startTag}'...v'${endVersion}  >> /tmp/proposed_changelog.txt;
echo '' >> /tmp/proposed_changelog.txt
touch CHANGE_LOG.md
grep -A 999 "^## " CHANGE_LOG.md  > /tmp/release_notes.txt
echo '# Change log' > /tmp/changelog.head
echo '' >> /tmp/changelog.head
echo 'All releases can be found at https://github.com/Aiven-Open/opensearch-connector-for-apache-kafka/releases' >> /tmp/changelog.head
echo '' >> /tmp/changelog.head
cat /tmp/changelog.head /tmp/proposed_changelog.txt /tmp/release_notes.txt > /tmp/CHANGE_LOG.md

# Strip trailing blank lines so the file ends with a single newline (spotless endWithNewline).
awk 'NF{last=NR} {line[NR]=$0} END{for(i=1;i<=last;i++) print line[i]}' /tmp/CHANGE_LOG.md > CHANGE_LOG.md

if ! git checkout -b "change_log-${endVersion}"; then
  echo "Failed to create changelog branch" >&2
  exit 1
fi

git add CHANGE_LOG.md
git commit -m "Update CHANGE_LOG.md for ${startTag} to v${endVersion}"

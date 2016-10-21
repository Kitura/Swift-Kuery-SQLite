/**
 Copyright IBM Corporation 2016

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

/// Describes where the location of the DB is stored
///
/// - inMemory:  This will be in memory and when the connection closes, the DB will vanish
/// - temporary: Will be a temporary on-disk DB, when the connection closes, it will be automatically deleted
/// - uri:       Where the DB stored
public enum Location {
    case inMemory
    case temporary
    case uri(String)
}

extension Location: CustomStringConvertible {
    public var description: String {
        switch self {
        case .inMemory:
            return ":memory:"
        case .temporary:
            return ""
        case .uri(let uri):
            return uri
        }
    }
}

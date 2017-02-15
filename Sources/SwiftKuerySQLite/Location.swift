/**
 Copyright IBM Corporation 2016, 2017

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

// MARK: Location

/// Location of the database.
public enum Location {
    /// In memory database, when the connection closes, the database will vanish.
    case inMemory
    
    /// A temporary on-disk database, when the connection closes, it will be automatically deleted.
    case temporary
    
    /// The URI where the database is stored.
    case uri(String)
}

extension Location: CustomStringConvertible {
    /// The textual representation of `Location`.
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

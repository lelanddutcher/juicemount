import Foundation

// Review #4 (R4 ReplyWalkProbe, ported with assertions): walk every POST and
// DELETE reply the service can send through the REAL decision mapping and
// the REAL EjectNowRequester in each GUI state (start, escalated, cancelled
// while an escalated POST is in flight), and check the outcome and the
// shutdown note App.swift would show are truthful. Build (repo root):
//
//   swiftc -parse-as-library app/JuiceMount/Sources/JuiceMount/Core/UnmountHelperService.swift \
//     app/JuiceMount/Tests/EjectNowReplyWalkProbe.swift -o <scratch>/rw && <scratch>/rw

typealias D = UnmountHelperDecision
func j(_ s: String) -> Data { Data(s.utf8) }

let posts: [(String, Int?, Data?)] = [
    ("200 RETIRED", 200, j(#"{"ok":true,"state":"RETIRED"}"#)),
    ("202 ordinary", 202, j(#"{"ok":false,"state":"ordinary_in_progress","retry_after":29}"#)),
    ("202 interrupted", 202, j(#"{"ok":false,"state":"ordinary_in_progress","retry_after":1,"error":"JuiceMount stopped its own eject attempt while this request was in flight; nothing was forced. Ejecting normally again"}"#)),
    ("202 no-retry", 202, j(#"{"ok":false,"state":"ordinary_in_progress"}"#)),
    ("409 withdrawn", 409, j(#"{"ok":false,"state":"withdrawn","error":"the eject was withdrawn (Quit was cancelled) while this request was in flight; nothing was forced"}"#)),
    ("409 NO_RECEIPT", 409, j(#"{"ok":false,"state":"NO_RECEIPT","error":"this service owns no mount registered with the unmount helper"}"#)),
    ("502 err", 502, j(#"{"ok":false,"state":"FAILED","error":"unmount helper ended op in state FAILED"}"#)),
    ("503 UNAVAILABLE", 503, j(#"{"ok":false,"state":"UNAVAILABLE","error":"unmount helper unavailable"}"#)),
    ("504 force pending", 504, j(#"{"ok":false,"state":"FORCE","error":"pending"}"#)),
    ("404", 404, nil), ("403 text", 403, j("missing local control header\n")),
    ("no answer", nil, nil),
]
let deletes: [(String, Int?, Data?)] = [
    ("200 RETIRED", 200, j(#"{"ok":true,"state":"RETIRED"}"#)),
    ("200 CANCELLED", 200, j(#"{"ok":true,"state":"CANCELLED"}"#)),
    ("200 REGISTERED", 200, j(#"{"ok":true,"state":"REGISTERED"}"#)),
    ("200 no state", 200, j(#"{"ok":true}"#)),
    ("409 force", 409, j(#"{"ok":false,"state":"force_in_progress","error":"x"}"#)),
    ("409 NO_RECEIPT", 409, j(#"{"ok":false,"state":"NO_RECEIPT","error":"no mount"}"#)),
    ("502", 502, j(#"{"ok":false,"error":"boom"}"#)),
    ("404", 404, nil), ("no answer", nil, nil),
]

@main struct EjectNowReplyWalkProbe {
    static func main() {
        var failures = 0
        func check(_ ok: Bool, _ what: String) {
            if !ok { print("FAIL: \(what)"); failures += 1 }
        }
        for (name, code, body) in posts {
            let r = D.ejectResult(httpStatus: code, body: body)
            // Start POST reply (not escalated): only RETIRED or a force already
            // under way (504) ends it; anything else is remembered.
            var a = EjectNowRequester(); _ = a.countdownStarted(); _ = a.reply(r)
            let startOK: Bool
            switch r {
            case .ejected: startOK = a.outcome == .retired
            case .forcePending(let m): startOK = a.outcome == .forcePending(m)
            default: startOK = a.outcome == nil
            }
            check(startOK, "start \(name): outcome \(String(describing: a.outcome))")
            // Escalated reply: the outcome and the note App.swift shows.
            var b = EjectNowRequester(); _ = b.countdownStarted(); _ = b.reply(.inProgress(retryAfter: 1)); _ = b.escalate(userPressed: true); _ = b.reply(r)
            let note = (b.outcome.flatMap { D.shutdownEjectNote(for: $0) } ?? "").lowercased()
            switch code {
            case 200?:
                check(b.outcome == .retired && note.isEmpty, "escalated \(name): \(String(describing: b.outcome))")
            case 202?:
                check(b.outcome == nil && b.waitRemaining != nil, "escalated \(name) keeps asking: \(String(describing: b.outcome))")
            case 404?:
                check(note.contains("not performed") && !note.contains("did not complete"), "escalated 404 note says nothing was attempted: \(note)")
            case nil:
                check(note.contains("unknown") && !note.contains("did not complete") && !note.contains("not performed"), "escalated no-answer note says the outcome is unknown: \(note)")
            case 504?:
                check(note.contains("unknown") && !note.contains("not performed"), "escalated 504 note: \(note)")
            default:
                check(note.contains("did not complete"), "escalated \(name) failure note: \(note)")
            }
            print("POST \(name) -> \(r) | escalated: \(String(describing: b.outcome)) note=\(note)")
            // Escalated while the START POST was in flight (review #5,
            // finding 2): that POST asked for an ordinary eject, so its
            // missing answer is never described as a possible force.
            var e = EjectNowRequester(); _ = e.countdownStarted(); _ = e.escalate(userPressed: true); _ = e.reply(r)
            let noteE = (e.outcome.flatMap { D.shutdownEjectNote(for: $0) } ?? "").lowercased()
            switch code {
            case nil:
                check(e.outcome == .startUnanswered("The background service did not answer.") && !noteE.contains("may have forced") && !noteE.contains("did not complete"), "start-in-flight no-answer: \(String(describing: e.outcome)) note=\(noteE)")
            case 202?:
                check(e.outcome == nil && e.waitRemaining != nil, "start-in-flight \(name) keeps asking: \(String(describing: e.outcome))")
            default:
                check(e.outcome == b.outcome, "start-in-flight \(name) matches escalated: \(String(describing: e.outcome)) vs \(String(describing: b.outcome))")
            }
            // Cancelled while an escalated POST is in flight: the reply never
            // ends the sequence except RETIRED; anything else sends DELETE.
            var c = EjectNowRequester(); _ = c.countdownStarted(); _ = c.reply(.inProgress(retryAfter: 1)); _ = c.escalate(userPressed: true)
            _ = c.cancel()
            let cmd = c.reply(r)
            check(r == .ejected ? (c.outcome == .retired && cmd == .none) : (cmd == .withdraw && c.outcome == nil), "cancel-inflight \(name): \(cmd) \(String(describing: c.outcome))")
        }
        for (name, code, body) in deletes {
            let w = D.withdrawResult(httpStatus: code, body: body)
            var c = EjectNowRequester(); _ = c.countdownStarted(); _ = c.reply(.inProgress(retryAfter: 1)); _ = c.cancel(); _ = c.withdrawReply(w)
            check(c.outcome == .abandoned(w), "DELETE \(name): \(String(describing: c.outcome))")
            if code == 409, name.contains("force") { check(c.ejectingMessage == EjectNowRequester.forceInProgressMessage, "DELETE 409 force text") }
            print("DELETE \(name) -> \(w) | outcome=\(String(describing: c.outcome))")
        }
        if failures > 0 { exit(1) }
        print("PASS: eject-now reply walk")
    }
}

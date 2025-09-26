#include <argparse.hpp>
#include <iostream>
#include <deque>
#include <cassert>
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <Windows.h>

#include <rlib/common.hpp>
#include <rlib/iofile.hpp>
#include <rlib/rcdn.hpp>
#include <rlib/rfile.hpp>

using namespace rlib;

namespace {
	struct alignas(64) ThreadState {
		enum class ProgressState {
			NOTHING,
			BEGIN,
			VERIFY,
			DOWNLOAD,
			DONE,
		};

		std::string path;
		ProgressState state = ProgressState::NOTHING;
		uint8_t progress;
        size_t progress_bytes;
	};
}

struct Main {
    struct CLI {
        std::string manifest = {};
        std::string updatefrommanfiest = {};
        std::string output = {};
        bool no_verify = {};
        bool no_write = {};
        bool no_progress = {};
        RFile::Match match = {};
        RCache::Options cache = {};
        RCDN::Options cdn = {};
        uint32_t job_count = 1;
    } cli = {};
    std::unique_ptr<RCache> cache = {};
    std::unique_ptr<RCDN> cdn = {};

    auto parse_args(int argc, char** argv) -> void {
        argparse::ArgumentParser program(fs::path(argv[0]).filename().generic_string());
        program.add_description("Downloads or repairs files in manifest.");
        // Common options
        program.add_argument("manifest").help("Manifest file to read from.").required();
        program.add_argument("output")
            .help("Output directory to store and verify files from.")
            .default_value(std::string("."));
        program.add_argument("-l", "--filter-lang")
            .help("Filter by language(none for international files) with regex match.")
            .default_value(std::optional<std::regex>{})
            .action([](std::string const& value) -> std::optional<std::regex> {
                if (value.empty()) {
                    return std::nullopt;
                } else {
                    return std::regex{value, std::regex::optimize | std::regex::icase};
                }
            });
        program.add_argument("-p", "--filter-path")
            .help("Filter by path with regex match.")
            .default_value(std::optional<std::regex>{})
            .action([](std::string const& value) -> std::optional<std::regex> {
                if (value.empty()) {
                    return std::nullopt;
                } else {
                    return std::regex{value, std::regex::optimize | std::regex::icase};
                }
            });
        program.add_argument("-u", "--update")
            .help("Filter: update from old manifest.")
            .default_value(std::string(""));
        program.add_argument("--no-verify")
            .help("Force force full without verify.")
            .default_value(false)
            .implicit_value(true);
        program.add_argument("--no-write").help("Do not write to file.").default_value(false).implicit_value(true);
        program.add_argument("--no-progress").help("Do not print progress.").default_value(false).implicit_value(true);

        // Cache options
        program.add_argument("--cache").help("Cache file path.").default_value(std::string{""});
        program.add_argument("--cache-readonly")
            .help("Do not write to cache.")
            .default_value(false)
            .implicit_value(true);
        program.add_argument("--cache-newonly")
            .help("Force create new part regardless of size.")
            .default_value(false)
            .implicit_value(true);
        program.add_argument("--cache-buffer")
            .help("Size for cache buffer in megabytes [1, 4096]")
            .default_value(std::uint32_t{32})
            .action([](std::string const& value) -> std::uint32_t {
                return std::clamp((std::uint32_t)std::stoul(value), 1u, 4096u);
            });
        program.add_argument("--cache-limit")
            .help("Size for cache bundle limit in gigabytes [0, 4096]")
            .default_value(std::uint32_t{4})
            .action([](std::string const& value) -> std::uint32_t {
                return std::clamp((std::uint32_t)std::stoul(value), 0u, 4096u);
            });

        // CDN options
        program.add_argument("--cdn")
            .help("Source url to download files from.")
            .default_value(std::string("http://lol.secure.dyn.riotcdn.net/channels/public"));
        program.add_argument("--cdn-lowspeed-time")
            .help("Curl seconds that the transfer speed should be below.")
            .default_value(std::size_t{0})
            .action([](std::string const& value) -> std::size_t { return (std::size_t)std::stoul(value); });
        program.add_argument("--cdn-lowspeed-limit")
            .help("Curl average transfer speed in killobytes per second that the transfer should be above.")
            .default_value(std::size_t{64})
            .action([](std::string const& value) -> std::size_t { return (std::size_t)std::stoul(value); });
        program.add_argument("--cdn-retry")
            .help("Number of retries to download from url.")
            .default_value(std::uint32_t{3})
            .action([](std::string const& value) -> std::uint32_t {
                return std::clamp((std::uint32_t)std::stoul(value), 0u, 8u);
            });
        program.add_argument("--cdn-workers")
            .default_value(std::uint32_t{32})
            .help("Number of connections per downloaded file.")
            .action([](std::string const& value) -> std::uint32_t {
                return std::clamp((std::uint32_t)std::stoul(value), 1u, 64u);
            });
        program.add_argument("--cdn-interval")
            .help("Curl poll interval in miliseconds.")
            .default_value(int{100})
            .action([](std::string const& value) -> int { return std::clamp((int)std::stoul(value), 0, 30000); });
        program.add_argument("--cdn-verbose").help("Curl: verbose logging.").default_value(false).implicit_value(true);
        program.add_argument("--cdn-buffer")
            .help("Curl buffer size in killobytes [1, 512].")
            .default_value(long{512})
            .action(
                [](std::string const& value) -> long { return std::clamp((long)std::stoul(value), 1l, 512l) * 1024; });
        program.add_argument("--cdn-proxy").help("Curl: proxy.").default_value(std::string{});
        program.add_argument("--cdn-useragent").help("Curl: user agent string.").default_value(std::string{});
        program.add_argument("--cdn-cookiefile")
            .help("Curl cookie file or '-' to disable cookie engine.")
            .default_value(std::string{});
        program.add_argument("--cdn-cookielist").help("Curl: cookie list string.").default_value(std::string{});

        // multithreading
        program.add_argument("-j", "--jobs")
            .help("Number of threads to verify and download")
            .default_value(1u)
            .action(
                [](std::string const& value) -> uint32_t { return std::clamp((uint32_t)std::stoul(value), 1u, 512u); });

        program.parse_args(argc, argv);

        cli.manifest = program.get<std::string>("manifest");
        cli.output = program.get<std::string>("output");
        cli.updatefrommanfiest = program.get<std::string>("--update");

        cli.no_verify = program.get<bool>("--no-verify");
        cli.no_write = program.get<bool>("--no-write");
        cli.no_progress = program.get<bool>("--no-progress");
        cli.match.langs = program.get<std::optional<std::regex>>("--filter-lang");
        cli.match.path = program.get<std::optional<std::regex>>("--filter-path");

        cli.cache = {
            .path = program.get<std::string>("--cache"),
            .readonly = program.get<bool>("--cache-readonly"),
            .newonly = program.get<bool>("--cache-newonly"),
            .flush_size = program.get<std::uint32_t>("--cache-buffer") * MiB,
            .max_size = program.get<std::uint32_t>("--cache-limit") * GiB,
        };

        cli.cdn = {
            .url = clean_path(program.get<std::string>("--cdn")),
            .verbose = program.get<bool>("--cdn-verbose"),
            .buffer = program.get<long>("--cdn-buffer"),
            .interval = program.get<int>("--cdn-interval"),
            .retry = program.get<std::uint32_t>("--cdn-retry"),
            .workers = program.get<std::uint32_t>("--cdn-workers"),
            .proxy = program.get<std::string>("--cdn-proxy"),
            .useragent = program.get<std::string>("--cdn-useragent"),
            .cookiefile = program.get<std::string>("--cdn-cookiefile"),
            .cookielist = program.get<std::string>("--cdn-cookielist"),
            .low_speed_limit = program.get<std::size_t>("--cdn-lowspeed-limit") * KiB,
            .low_speed_time = program.get<std::size_t>("--cdn-lowspeed-time"),
        };

        cli.job_count = program.get<uint32_t>("--jobs");
    }

    auto run() -> void {
        rlib_trace("Manifest file: %s", cli.manifest.c_str());
        if (!RFile::has_known_bundle(cli.manifest)) {
            cli.cdn.url.clear();
        }

        if (cli.cdn.url.empty()) {
            cli.cache.readonly = true;
        }

        if (!cli.no_write) {
            fs::create_directories(cli.output);
        }

        if (!cli.cache.path.empty()) {
            cache = std::make_unique<RCache>(cli.cache);
        }

        cdn = std::make_unique<RCDN>(cli.cdn, cache.get());

        auto skipids = std::unordered_map<std::string, FileID>{};
        if (!cli.updatefrommanfiest.empty()) {
            rlib_trace("Update from file: %s", cli.updatefrommanfiest.c_str());
            RFile::read_file(cli.updatefrommanfiest, [&, this](RFile const& rfile) {
                if (cli.match(rfile)) {
                    skipids[rfile.path] = rfile.fileId;
                }
                return true;
            });
        }

        auto files = std::vector<RFile>{};
        RFile::read_file(cli.manifest, [&, this](RFile& rfile) {
            if (auto i = skipids.find(rfile.path); i != skipids.cend() &&  i->second == rfile.fileId) {
                return true;
            }
            if (cli.match(rfile)) {
                files.emplace_back(std::move(rfile));
            }
            return true;
        });

        if (cli.job_count > 1) {
            SetConsoleMode(GetStdHandle(STD_ERROR_HANDLE),
                           ENABLE_PROCESSED_OUTPUT | ENABLE_WRAP_AT_EOL_OUTPUT | ENABLE_VIRTUAL_TERMINAL_PROCESSING);
            SetConsoleMode(GetStdHandle(STD_OUTPUT_HANDLE),
                           ENABLE_PROCESSED_OUTPUT | ENABLE_WRAP_AT_EOL_OUTPUT | ENABLE_VIRTUAL_TERMINAL_PROCESSING);

            struct FinishedItem {
                enum class State { FAIL, OK };
                std::string path;
                State state;
                size_t size;
            };

            std::vector<std::thread> threads;
            std::vector<ThreadState> thread_states{};
            std::deque<FinishedItem> finished_items;
            std::mutex state_queue_mutex;

            for (size_t i = 0; i < cli.job_count; ++i) {
                thread_states.push_back(ThreadState{});
            }

            size_t combined_size = 0;
            for (size_t i = 0; i < files.size(); ++i) {
                combined_size += files[i].size;
            }

            size_t file_count = files.size();
            size_t files_per_thread = std::max(file_count / cli.job_count, 1ull);
            size_t bytes_per_thread = std::max(combined_size / cli.job_count, 1ull);
            size_t cur_off = 0;
            // TODO: need to rebalance if some threads gets lots of large files
            for (size_t i = 0; i < cli.job_count; ++i) {
                size_t start_off = cur_off;
                size_t end_off = start_off;
                size_t cur_bytes = 0;
                while (cur_bytes < bytes_per_thread) {
                    if (end_off >= files.size()) {
                        break;
                    }
                    cur_bytes += files[end_off].size;
                    ++end_off;
                }
                if (i == cli.job_count - 1) {
                    end_off = files.size();
                }
                cur_off = end_off;

                #if 0
                size_t end_off = std::min(start_off + files_per_thread, file_count);
                cur_off += files_per_thread;
                #endif
                if (end_off - start_off == 0) {
                    thread_states[i].state = ThreadState::ProgressState::DONE;
                    break;
                }

                ThreadState& state = thread_states[i];
                const auto* file_ptrs = files.data();

                threads.push_back(
                    std::thread{[this, start_off, end_off, file_ptrs, &state, &finished_items, &state_queue_mutex] {
						auto rcdn = std::make_unique<RCDN>(cli.cdn, cache.get());
                        for (size_t i = start_off; i < end_off; ++i) {
                            auto const& rfile = file_ptrs[i];

                            {
                                std::scoped_lock g{state_queue_mutex};
                                state.path = rfile.path;
                                state.progress = 0;
                                state.state = ThreadState::ProgressState::BEGIN;
                            }
                            bool res = download_file_mt(rfile, i, state, rcdn.get());

                            std::scoped_lock g{state_queue_mutex};
                            finished_items.push_back(
                                FinishedItem{.path = std::move(state.path),
                                             .state = (res ? FinishedItem::State::OK : FinishedItem::State::FAIL),
                                .size = rfile.size});
                            state.state = ThreadState::ProgressState::NOTHING;
                        }

                        std::scoped_lock g{state_queue_mutex};
                        state.state = ThreadState::ProgressState::DONE;
                    }});
            }

            size_t last_bar_count = 0;
            size_t finished_item_count = 0, finished_item_size = 0;
            char buf[256];
            std::vector<std::string> lines;
            while (true) {
                if (last_bar_count) {
                    std::snprintf(buf, sizeof(buf), "\x1B[%lluF\r", last_bar_count);
                    std::cerr << buf;
                }

                size_t finished_lines_printed = 0;
                {
                    while (true) {
						std::unique_lock g{state_queue_mutex};
                        if (finished_items.empty()) {
                            break;
                        }

                        FinishedItem item = std::move(finished_items.front());
                        finished_items.pop_front();
                        g.unlock();

                        std::snprintf(buf,
                                      sizeof(buf),
                                      "\x1B[2K%s: %s\n",
                                      item.state == FinishedItem::State::OK ? "OK" : "FAIL",
                                      item.path.c_str());
                        std::cout << buf;
                        ++finished_lines_printed;
                        ++finished_item_count;
                        finished_item_size += item.size;
                    }
                }

                lines.clear();
                bool all_done = true;
                for (size_t i = 0; i < cli.job_count; ++i) {
                    ThreadState& state = thread_states[i];
                    {
                        std::scoped_lock g{state_queue_mutex};
                        if (state.state == ThreadState::ProgressState::DONE) {
                            continue;
                        }

                        all_done = false;

                        if (state.state == ThreadState::ProgressState::NOTHING) {
                            continue;
                        }
                        const char* state_str = "";
                        switch (state.state) {
                            using enum ThreadState::ProgressState;
                            case BEGIN:
                                state_str = "BEGIN";
                                break;
                            case DOWNLOAD:
                                state_str = "DOWNLOAD";
                                break;
                            case VERIFY:
                                state_str = "VERIFY";
                                break;
                            default:
                                assert(0);
                        }

                        float progress_mb = (float)state.progress_bytes / (1024.f * 1024.f);
                        size_t missing_spaces = 0;
                        if (state.path.size() < 80) {
                            missing_spaces = 80 - state.path.size();
                        }
                        std::snprintf(buf, sizeof(buf), "\x1B[2K%s:%*c %s %7.02fMB %02u%%\n", state.path.c_str(), (int)missing_spaces, ' ', state_str, progress_mb, state.progress);
                    }
                    lines.push_back(buf);
                }

                if (lines.size() + finished_lines_printed + 1 < last_bar_count) {
                    size_t target = last_bar_count - (lines.size() + finished_lines_printed + 1);
                    for (size_t i = 0; i < target; ++i) {
                        std::cerr << "\x1B[2K\n";
                    }
                }

                for (auto& line : lines) {
                    std::cerr << line;
                }

                float finished_gb = (float)finished_item_size / (1024.f * 1024.f * 1024.f);
                float combined_gb = (float)combined_size / (1024.f * 1024.f * 1024.f);
                std::snprintf(buf,
                              sizeof(buf),
                              "\x1B[2KFiles Processed: %6llu/%6llu   %6.02fGB/%6.02fGB\n",
                              finished_item_count,
                              files.size(),
                              finished_gb,
                              combined_gb);
                std::cerr << buf;
                last_bar_count = lines.size() + 1;

                if (all_done) {
					std::cerr << "FINISHED\n";
                    break;
                }

                std::this_thread::sleep_for(std::chrono::milliseconds{50});
            }

            for (auto& thread : threads) {
                thread.join();
            }

            return;
        }


        for (std::uint32_t index = files.size(); auto const& rfile : files) {
            download_file(rfile, index--);
        }
    }

    auto download_file(RFile const& rfile, std::uint32_t index) -> void {
        std::cout << "START: " << rfile.path << std::endl;
        auto path = fs::path(cli.output) / rfile.path;
        rlib_trace("Path: %s", path.generic_string().c_str());
        auto done = std::uint64_t{};
        auto bad_chunks = std::vector<RChunk::Dst>{};

        if (!rfile.chunks) {
            if (rfile.size) {
                rlib_assert(cache.get());
                bad_chunks = cache->get_chunks(rfile.fileId);
                rlib_assert(!bad_chunks.empty());
            }
        } else {
            bad_chunks = *rfile.chunks;
        }

        if (!cli.no_verify && !bad_chunks.empty()) {
            progress_bar p("VERIFIED", cli.no_progress, index, done, rfile.size);
            RChunk::Dst::verify(path, bad_chunks, [&](RChunk::Dst const& chunk, std::span<char const> data) {
                done += chunk.uncompressed_size;
                p.update(done);
            });
        }

        auto outfile = std::unique_ptr<IO::File>();
        if (!cli.no_write) {
            outfile = std::make_unique<IO::File>(path, IO::WRITE);
            rlib_assert(outfile->resize(0, rfile.size));
        }

        if (!bad_chunks.empty() && cdn) {
            progress_bar p("DOWNLOAD", cli.no_progress, index, done, rfile.size);
            bad_chunks = cdn->get(std::move(bad_chunks), [&](RChunk::Dst const& chunk, std::span<char const> data) {
                if (outfile) {
                    rlib_assert(outfile->write(chunk.uncompressed_offset, data));
                }
                done += chunk.uncompressed_size;
                p.update(done);
            });
        }

        if (!bad_chunks.empty()) {
            std::cout << "FAIL!" << std::endl;
        } else {
            if (outfile) {
                outfile = nullptr;
                if (rfile.permissions & 01) {
                    fs::permissions(path,
                                    fs::perms::owner_exec | fs::perms::group_exec | fs::perms::others_exec,
                                    fs::perm_options::add);
                }
            }
            std::cout << "OK!" << std::endl;
        }
    }

    auto download_file_mt(RFile const& rfile, std::uint32_t index, ThreadState& state, RCDN* rcdn) -> bool {
        auto path = fs::path(cli.output) / rfile.path;
        rlib_trace("Path: %s", path.generic_string().c_str());
        auto done = std::uint64_t{};
        auto bad_chunks = std::vector<RChunk::Dst>{};

        if (!rfile.chunks) {
            if (rfile.size) {
                rlib_assert(cache.get());
                bad_chunks = cache->get_chunks(rfile.fileId);
                rlib_assert(!bad_chunks.empty());
            }
        } else {
            bad_chunks = *rfile.chunks;
        }

        if (!cli.no_verify && !bad_chunks.empty()) {
            // progress_bar p("VERIFIED", cli.no_progress, index, done, rfile.size);
            state.progress = 0;
            state.state = ThreadState::ProgressState::VERIFY;
            RChunk::Dst::verify(path, bad_chunks, [&](RChunk::Dst const& chunk, std::span<char const> data) {
                done += chunk.uncompressed_size;
                uint32_t progress = (done * 100) / rfile.size;
                state.progress = progress;
                state.progress_bytes = done;
            });
        }

        auto outfile = std::unique_ptr<IO::File>();
        if (!cli.no_write) {
            outfile = std::make_unique<IO::File>(path, IO::WRITE);
            rlib_assert(outfile->resize(0, rfile.size));
        }

        if (!bad_chunks.empty() && cdn) {
            //progress_bar p("DOWNLOAD", cli.no_progress, index, done, rfile.size);
            state.progress = 0;
            state.state = ThreadState::ProgressState::DOWNLOAD;
            bad_chunks = rcdn->get(std::move(bad_chunks), [&](RChunk::Dst const& chunk, std::span<char const> data) {
                if (outfile) {
                    rlib_assert(outfile->write(chunk.uncompressed_offset, data));
                }
                done += chunk.uncompressed_size;
                uint32_t progress = (done * 100) / rfile.size;
                state.progress = progress;
                state.progress_bytes = done;
            });
        }

        if (!bad_chunks.empty()) {
            return false;
        } else {
            if (outfile) {
                outfile = nullptr;
                if (rfile.permissions & 01) {
                    fs::permissions(path,
                                    fs::perms::owner_exec | fs::perms::group_exec | fs::perms::others_exec,
                                    fs::perm_options::add);
                }
            }
            return true;
        }
    }
};

int main(int argc, char** argv) {
    auto main = Main{};
    try {
        main.parse_args(argc, argv);
        main.run();
    } catch (std::exception const& e) {
        std::cerr << e.what() << std::endl;
        for (auto const& error : error_stack()) {
            std::cerr << error << std::endl;
        }
        error_stack().clear();
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
